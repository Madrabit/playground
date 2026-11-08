package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/scheduler"
)

type httpErrorMock struct {
	addErr error
	getErr error
}

func (m *httpErrorMock) Add(job domain.Job) error {
	return m.addErr
}

func (m *httpErrorMock) GetById(id int) (domain.Job, error) {
	return domain.Job{}, m.getErr
}

func (m *httpErrorMock) Stats() scheduler.SchedulerStats {
	return scheduler.SchedulerStats{}
}

func TestHTTPErrorStatuses(t *testing.T) {
	const internalDetails = "secret database connection details"

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		err        error
		wantStatus int
	}{
		{
			name:       "duplicate job",
			method:     http.MethodPost,
			path:       "/jobs",
			body:       `{"name":"test","complexity":1}`,
			err:        fmt.Errorf("%s: %w", internalDetails, ErrDuplicateJob),
			wantStatus: http.StatusConflict,
		},
		{
			name:       "job not found",
			method:     http.MethodGet,
			path:       "/jobs/42",
			err:        fmt.Errorf("%s: %w", internalDetails, ErrJobNotFound),
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "validation error",
			method:     http.MethodPost,
			path:       "/jobs",
			body:       `{"name":"test","complexity":1}`,
			err:        fmt.Errorf("%s: %w", internalDetails, ErrValidation),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "queue full",
			method:     http.MethodPost,
			path:       "/jobs",
			body:       `{"name":"test","complexity":1}`,
			err:        fmt.Errorf("%s: %w", internalDetails, ErrQueueFull),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "internal error",
			method:     http.MethodPost,
			path:       "/jobs",
			body:       `{"name":"test","complexity":1}`,
			err:        errors.New(internalDetails),
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &httpErrorMock{
				addErr: tt.err,
				getErr: tt.err,
			}

			handler := NewHttpHandler(
				func() bool { return true },
				mock,
			)

			req := httptest.NewRequest(
				tt.method,
				tt.path,
				strings.NewReader(tt.body),
			)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf(
					"status = %d, want %d; body = %q",
					rec.Code,
					tt.wantStatus,
					rec.Body.String(),
				)
			}

			// Внутренние подробности не должны попасть в HTTP-ответ.
			if strings.Contains(rec.Body.String(), internalDetails) {
				t.Errorf(
					"internal error leaked: %q",
					rec.Body.String(),
				)
			}

			wantBody := http.StatusText(tt.wantStatus)

			if strings.TrimSpace(rec.Body.String()) != wantBody {
				t.Errorf(
					"body = %q, want %q",
					rec.Body.String(),
					wantBody,
				)
			}
		})
	}
}
