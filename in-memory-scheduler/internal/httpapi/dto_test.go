// ИЗМЕНЕНО В ЗАДАЧЕ 28.1: проверки presence и JSON-контрактов HTTP DTO.
package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"playground/in-memory-scheduler/internal/domain"
)

// Compare the wire format as well as DTO values: decoding into a struct alone
// would silently ignore leaked fields and tolerate incorrect field casing.
func assertJSONResponse(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	assertJSONObject(t, rec.Body.Bytes(), want)
}

func assertJSONObject(t *testing.T, body []byte, want string) {
	t.Helper()
	var gotJSON, wantJSON map[string]json.RawMessage
	if err := json.Unmarshal(body, &gotJSON); err != nil {
		t.Fatalf("decode JSON: %v; body = %q", err, body)
	}
	if err := json.Unmarshal([]byte(want), &wantJSON); err != nil {
		t.Fatalf("invalid expected JSON: %v", err)
	}
	if len(gotJSON) != len(wantJSON) {
		t.Errorf("JSON has %d fields, want %d: %s", len(gotJSON), len(wantJSON), body)
	}
	for key, wantValue := range wantJSON {
		gotValue, ok := gotJSON[key]
		if !ok {
			t.Errorf("JSON is missing field %q: %s", key, body)
			continue
		}
		if !reflect.DeepEqual(bytes.TrimSpace(gotValue), bytes.TrimSpace(wantValue)) {
			t.Errorf("JSON field %q = %s, want %s", key, gotValue, wantValue)
		}
	}
}

func TestCreateJobFieldPresence(t *testing.T) {
	tests := []struct {
		name            string
		body            string
		wantStatus      int
		wantCalls       int
		wantName        string
		wantDescription string
		wantComplexity  int
	}{
		{
			name: "missing complexity", body: `{"name":"test"}`,
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "null complexity", body: `{"name":"test","complexity":null}`,
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "explicit zero reaches domain validation", body: `{"name":"test","complexity":0}`,
			wantStatus: http.StatusCreated, wantCalls: 1, wantName: "test",
		},
		{
			name: "omitted strings", body: `{"complexity":1}`,
			wantStatus: http.StatusCreated, wantCalls: 1, wantComplexity: 1,
		},
		{
			name: "empty strings", body: `{"name":"","description":"","complexity":1}`,
			wantStatus: http.StatusCreated, wantCalls: 1, wantComplexity: 1,
		},
		{
			name: "explicit strings", body: `{"name":"test","description":"description","complexity":2}`,
			wantStatus: http.StatusCreated, wantCalls: 1,
			wantName: "test", wantDescription: "description", wantComplexity: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The mock accepts all jobs so this tests transport presence checks,
			// independently of the scheduler's name and complexity validation.
			fake := &mockScheduler{}
			handler := NewHttpHandler(func() bool { return true }, fake)
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if fake.addCalls != tt.wantCalls {
				t.Fatalf("Add calls = %d, want %d", fake.addCalls, tt.wantCalls)
			}
			if tt.wantCalls == 0 {
				assertJSONResponse(t, rec, `{"error":"complexity is required"}`)
				return
			}
			if fake.addJob.Name != tt.wantName || fake.addJob.Description != tt.wantDescription || fake.addJob.Complexity != tt.wantComplexity {
				t.Errorf("Add received %+v, want name=%q description=%q complexity=%d", fake.addJob, tt.wantName, tt.wantDescription, tt.wantComplexity)
			}
			assertJSONResponse(t, rec, fmt.Sprintf(`{"name":%q}`, tt.wantName))
		})
	}
}

func TestCreateJobRejectsInternalFields(t *testing.T) {
	for _, field := range []string{`"id":42`, `"state":1`, `"someSlice":["internal"]`, `"unknown":true`} {
		t.Run(field, func(t *testing.T) {
			fake := &mockScheduler{}
			handler := NewHttpHandler(func() bool { return true }, fake)
			body := `{"name":"test","complexity":1,` + field + `}`
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || fake.addCalls != 0 {
				t.Fatalf("status = %d, Add calls = %d; want 400 and no calls", rec.Code, fake.addCalls)
			}
			assertJSONResponse(t, rec, `{"error":"invalid JSON body"}`)
		})
	}
}

func TestJobResponseStateAndZeroValues(t *testing.T) {
	tests := []struct {
		state domain.JobStatus
		want  string
	}{
		{domain.Pending, "pending"},
		{domain.Running, "running"},
		{domain.Failed, "failed"},
		{domain.Done, "done"},
		{domain.Cancelled, "cancelled"},
		{domain.Stopped, "stopped"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			fake := &mockScheduler{jobToReturn: domain.Job{ID: 1, State: tt.state}}
			handler := NewHttpHandler(func() bool { return true }, fake)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/1", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			assertJSONResponse(t, rec, fmt.Sprintf(`{"id":1,"name":"","description":"","complexity":0,"attrs":{},"tags":{},"state":%q}`, tt.want))
		})
	}
}

func TestStatsResponseIncludesZeroCounters(t *testing.T) {
	handler := NewHttpHandler(func() bool { return true }, &mockScheduler{})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	assertJSONResponse(t, rec, `{"done":0,"failed":0,"cancelled":0}`)
}
