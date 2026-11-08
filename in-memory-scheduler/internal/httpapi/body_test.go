package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/scheduler"
)

type bodyLimitMockScheduler struct {
	addCalls int
}

func (m *bodyLimitMockScheduler) Add(job domain.Job) error {
	m.addCalls++
	return nil
}

func (m *bodyLimitMockScheduler) GetById(id int) (domain.Job, error) {
	return domain.Job{}, nil
}

func (m *bodyLimitMockScheduler) Stats() scheduler.SchedulerStats {
	return scheduler.SchedulerStats{}
}

func TestCreateJobBodyLimits(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCalls  int
	}{
		{
			name: "body exceeds limit",
			body: `{"name":"` +
				strings.Repeat("x", int(maxJobBodyBytes)) +
				`"}`,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCalls:  0,
		},
		{
			name: "valid JSON with oversized trailing data",
			body: `{"name":"test"}` +
				strings.Repeat(" ", int(maxJobBodyBytes)),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCalls:  0,
		},
		{
			name:       "malformed JSON",
			body:       `{"name":`,
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:       "multiple JSON objects",
			body:       `{"name":"first"}{"name":"second"}`,
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:       "unknown JSON field",
			body:       `{"name":"test","unknown":123}`,
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:       "valid body",
			body:       `{"name":"test","complexity":1}`,
			wantStatus: http.StatusCreated,
			wantCalls:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &bodyLimitMockScheduler{}

			handler := NewHttpHandler(
				func() bool { return true },
				mock,
			)

			req := httptest.NewRequest(
				http.MethodPost,
				"/jobs",
				strings.NewReader(tt.body),
			)
			req.Header.Set("Content-Type", "application/json")

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

			if mock.addCalls != tt.wantCalls {
				t.Errorf(
					"Add called %d times, want %d",
					mock.addCalls,
					tt.wantCalls,
				)
			}
		})
	}
}
