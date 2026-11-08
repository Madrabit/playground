package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/httpdto"
	"strconv"
)

const maxJobBodyBytes int64 = 1 << 20 // 1 MiB

type JobHandler struct {
	scheduler SchedulerAPI
}

func NewJobHandler(scheduler SchedulerAPI) *JobHandler {
	return &JobHandler{
		scheduler,
	}
}

func (j *JobHandler) handleCreateJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	r.Body = http.MaxBytesReader(w, r.Body, maxJobBodyBytes)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req httpdto.CreateJobRequest

	if err := decoder.Decode(&req); err != nil {
		writeBodyError(w, err)
		return
	}

	var extra json.RawMessage

	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			writeJSONError(
				w,
				http.StatusBadRequest,
				"body must contain exactly one JSON value",
			)
			return
		}

		writeBodyError(w, err)
		return
	}

	// Complexity обязателен, но ноль как значение
	// может быть передан отдельно от отсутствующего поля.
	if req.Complexity == nil {
		writeJSONError(
			w,
			http.StatusUnprocessableEntity,
			"complexity is required",
		)
		return
	}

	job := domain.Job{
		Name:        req.Name,
		Description: req.Description,
		Complexity:  *req.Complexity,
		Attrs:       req.Attrs,
		Tags:        req.Tags,
	}

	if err := j.scheduler.Add(job); err != nil {
		writeSchedulerError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, httpdto.CreateJobResponse{
		Name: req.Name,
	})
}

func (j *JobHandler) handleGetJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	jobID := r.PathValue("id")

	id, err := strconv.Atoi(jobID)
	if err != nil || id <= 0 {
		writeJSONError(
			w,
			http.StatusBadRequest,
			"invalid job id",
		)
		return
	}

	job, err := j.scheduler.GetById(id)
	if err != nil {
		writeSchedulerError(w, err)
		return
	}

	response := toJobResponse(job)
	writeJSON(w, http.StatusOK, response)
}

func (j *JobHandler) handleStats(
	w http.ResponseWriter,
	r *http.Request,
) {
	stats := j.scheduler.Stats()

	response := httpdto.StatsResponse{
		Done:      stats.Done,
		Failed:    stats.Failed,
		Cancelled: stats.Cancelled,
	}

	writeJSON(w, http.StatusOK, response)
}
