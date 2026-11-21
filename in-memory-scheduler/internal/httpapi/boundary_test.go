// ИЗМЕНЕНО В ЗАДАЧЕ 28.1: проверяется единая HTTP-граница для ошибок и Job DTO.
package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/scheduler"
)

type waitResponseScheduler struct {
	job     domain.Job
	initial error
	poll    error
	calls   int
}

func (s *waitResponseScheduler) Add(domain.Job) error { return nil }

func (s *waitResponseScheduler) Stats() scheduler.SchedulerStats {
	return scheduler.SchedulerStats{}
}

func (s *waitResponseScheduler) GetById(id int) (domain.Job, error) {
	s.calls++
	if s.calls == 1 {
		return domain.Job{ID: id, State: domain.Pending}, s.initial
	}
	return s.job, s.poll
}

func assertBoundaryResponse(t *testing.T, rec *httptest.ResponseRecorder, status int, want string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, status, rec.Body.String())
	}
	assertJSONResponse(t, rec, want)
}

func TestWaitJobHTTPContract(t *testing.T) {
	for _, tt := range []struct {
		name    string
		id      string
		initial error
		poll    error
		status  int
		body    string
	}{
		{
			name:   "job response excludes internal fields",
			id:     "42",
			status: http.StatusOK,
			body:   `{"id":42,"name":"test","description":"","complexity":0,"attrs":{},"tags":{},"state":"3"}`,
		},
		{
			name:   "invalid id",
			id:     "invalid",
			status: http.StatusBadRequest,
			body:   `{"error":"invalid job id"}`,
		},
		{
			name:    "initial lookup error",
			id:      "42",
			initial: fmt.Errorf("private storage details: %w", ErrJobNotFound),
			status:  http.StatusNotFound,
			body:    `{"error":"Not Found"}`,
		},
		{
			name:   "polling error",
			id:     "42",
			poll:   errors.New("private storage details"),
			status: http.StatusInternalServerError,
			body:   `{"error":"Internal Server Error"}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := &waitResponseScheduler{
				job: domain.Job{
					ID:        42,
					Name:      "test",
					State:     domain.Done,
					SomeSlice: []string{"private data"},
				},
				initial: tt.initial,
				poll:    tt.poll,
			}
			req := httptest.NewRequest(http.MethodGet, "/jobs/"+tt.id+"/wait", nil)
			req.SetPathValue("id", tt.id)
			rec := httptest.NewRecorder()
			NewJobHandler(fake).handleWaitJob(rec, req)
			assertBoundaryResponse(t, rec, tt.status, tt.body)
		})
	}
}

func TestReadinessErrorDTO(t *testing.T) {
	rec := httptest.NewRecorder()
	handler := NewHttpHandler(func() bool { return false }, &mockScheduler{})
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assertBoundaryResponse(t, rec, http.StatusServiceUnavailable, `{"error":"not ready"}`)
}

func TestRecoveryErrorDTO(t *testing.T) {
	handler := Recovery(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("private panic details")
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/42", nil))
	assertBoundaryResponse(t, rec, http.StatusInternalServerError, `{"error":"Internal Server Error"}`)
}

func TestConcurrencyLimitErrorDTO(t *testing.T) {
	var handler http.Handler
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/jobs/42", nil)
	handler = ConcurrencyLimit(1)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		// The outer request holds the only slot while this request is rejected.
		handler.ServeHTTP(rec, req)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), req)
	assertBoundaryResponse(t, rec, http.StatusServiceUnavailable, `{"error":"Service Unavailable"}`)
}
