package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/httpdto"
	"playground/in-memory-scheduler/internal/scheduler"
	"reflect"
	"testing"
)

type mockScheduler struct {
	addJob      domain.Job
	addCalls    int
	stats       scheduler.SchedulerStats
	jobToReturn domain.Job
	requestId   int
}

func (s *mockScheduler) Add(job domain.Job) error {
	s.addJob = job
	s.addCalls++
	return nil
}

func (s *mockScheduler) GetById(id int) (domain.Job, error) {
	s.requestId = id
	return s.jobToReturn, nil
}

func (s *mockScheduler) Stats() scheduler.SchedulerStats {
	return s.stats
}

func TestHandleCreateJob(t *testing.T) {
	fake := &mockScheduler{}

	complexity := 3
	jobReq := httpdto.CreateJobRequest{
		Name:        "test name",
		Description: "test description",
		Complexity:  &complexity,
		Attrs:       map[string]string{"priority": "high"},
		Tags:        map[string]struct{}{"batch": {}},
	}

	data, err := json.Marshal(jobReq)
	if err != nil {
		t.Fatalf("marshalling failed: %v", err)
	}
	assertJSONObject(t, data, `{
		"name":"test name","description":"test description","complexity":3,
		"attrs":{"priority":"high"},"tags":{"batch":{}}
	}`)
	handler := NewHttpHandler(
		func() bool {
			return true
		},
		fake,
	)
	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		bytes.NewReader(data),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf(
			"got %d, want %d, body: %s",
			rec.Code,
			http.StatusCreated,
			rec.Body.String(),
		)
	}
	assertJSONResponse(t, rec, `{"name":"test name"}`)
	var got httpdto.CreateJobResponse
	err = json.Unmarshal(rec.Body.Bytes(), &got)
	if err != nil {
		t.Fatalf("unmarshalling failed: %v", err)
	}
	if got.Name != jobReq.Name {
		t.Fatalf("got %s, want %s", got.Name, jobReq.Name)
	}
	wantJob := domain.Job{
		Name:        jobReq.Name,
		Description: jobReq.Description,
		Complexity:  complexity,
		Attrs:       jobReq.Attrs,
		Tags:        jobReq.Tags,
	}
	if fake.addCalls != 1 || !reflect.DeepEqual(fake.addJob, wantJob) {
		t.Fatalf("Add called %d times with %+v, want once with %+v", fake.addCalls, fake.addJob, wantJob)
	}
}

func TestHandleStats(t *testing.T) {
	fake := &mockScheduler{
		stats: scheduler.SchedulerStats{
			Done:      10,
			Cancelled: 2,
			Failed:    3,
		},
	}
	handler := NewHttpHandler(
		func() bool {
			return true
		},
		fake,
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/stats",
		nil,
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("got %d, want %d", rec.Code, http.StatusOK)
	}

	assertJSONResponse(t, rec, `{"done":10,"cancelled":2,"failed":3}`)
	var got httpdto.StatsResponse

	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Errorf("failed to unmarshal stats response: %v", err)
	}

	want := httpdto.StatsResponse{Done: 10, Cancelled: 2, Failed: 3}
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestHandleGetJobById(t *testing.T) {

	fake := &mockScheduler{}
	expected := domain.Job{
		ID:          42,
		Name:        "test name",
		Description: "test description",
		Complexity:  4,
		Attrs:       map[string]string{"priority": "high"},
		Tags:        map[string]struct{}{"batch": {}},
		SomeSlice:   []string{"internal-only data"},
		State:       domain.Running,
	}
	fake.jobToReturn = expected
	handler := NewHttpHandler(
		func() bool {
			return true
		},
		fake,
	)
	req := httptest.NewRequest(
		http.MethodGet,
		"/jobs/42",
		nil,
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("got %d, want %d", rec.Code, http.StatusOK)
	}
	assertJSONResponse(t, rec, `{
		"id":42,"name":"test name","description":"test description",
		"complexity":4,"attrs":{"priority":"high"},"tags":{"batch":{}},"state":"running"
	}`)
	var got httpdto.JobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Errorf("failed to unmarshal job response: %v", err)
	}
	if fake.requestId != got.ID {
		t.Errorf("got %d, want %d", got.ID, fake.requestId)
	}
	want := httpdto.JobResponse{
		ID:          expected.ID,
		Name:        expected.Name,
		Description: expected.Description,
		Complexity:  expected.Complexity,
		Attrs:       expected.Attrs,
		Tags:        expected.Tags,
		State:       "running",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
