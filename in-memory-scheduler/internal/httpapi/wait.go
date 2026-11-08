package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"playground/in-memory-scheduler/internal/domain"
)

func (j *JobHandler) handleWaitJob(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id <= 0 {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	// Запоминаем состояние задачи на момент начала запроса.
	initial, err := j.scheduler.GetById(id)
	if err != nil {
		status := ToHTTPStatus(err)
		http.Error(w, http.StatusText(status), status)
		return
	}

	// Ждём не дольше 10 секунд.
	// Если клиент отменит запрос, ctx тоже отменится.
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	updated, err := j.waitForStateChange(ctx, id, initial.State)

	if errors.Is(err, context.Canceled) {
		// Клиент отключился. Отвечать ему уже не нужно.
		return
	}

	if errors.Is(err, context.DeadlineExceeded) {
		// За 10 секунд состояние не изменилось.
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if err != nil {
		status := ToHTTPStatus(err)
		http.Error(w, http.StatusText(status), status)
		return
	}

	data, err := json.Marshal(updated)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// Это функция, которая действительно выполняет ожидание.
// Именно сюда мы передаём context из HTTP-запроса.
func (j *JobHandler) waitForStateChange(
	ctx context.Context,
	id int,
	initialState domain.JobStatus,
) (domain.Job, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return domain.Job{}, ctx.Err()

		case <-ticker.C:
			job, err := j.scheduler.GetById(id)
			if err != nil {
				return domain.Job{}, err
			}

			if job.State != initialState {
				return job, nil
			}
		}
	}
}
