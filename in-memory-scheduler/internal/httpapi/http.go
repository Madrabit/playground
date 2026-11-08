package httpapi

import (
	"errors"
	"net/http"
	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/scheduler"
)

type SchedulerAPI interface {
	Add(job domain.Job) error
	GetById(id int) (domain.Job, error)
	Stats() scheduler.SchedulerStats
}

func ToHTTPStatus(err error) int {
	switch {
	case errors.Is(err, ErrJobNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrDuplicateJob):
		return http.StatusConflict
	case errors.Is(err, ErrQueueFull):
		return http.StatusServiceUnavailable
	case errors.Is(err, ErrValidation):
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}

}

func NewHttpHandler(isReady func() bool, scheduler SchedulerAPI) http.Handler {
	mux := http.NewServeMux()
	handler := NewJobHandler(scheduler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !isReady() {
			http.Error(
				w,
				"not ready",
				http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /jobs", handler.handleCreateJob)
	mux.HandleFunc("GET /jobs/{id}", handler.handleGetJob)
	mux.HandleFunc("GET /stats", handler.handleStats)
	return mux
}
