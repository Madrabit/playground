package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/scheduler"
	"reflect"
	"testing"
)

type mockScheduler struct {
	addJob      domain.Job
	stats       scheduler.SchedulerStats
	jobToReturn domain.Job
	requestId   int
}

func (s *mockScheduler) Add(job domain.Job) error {
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

	jobReq := CreateJobReq{
		Name:        "test name",
		Description: "test description",
	}

	data, err := json.Marshal(jobReq)
	if err != nil {
		t.Fatalf("marshalling failed: %v", err)
	}
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
	var got CreateJobResp
	err = json.Unmarshal(rec.Body.Bytes(), &got)
	if err != nil {
		t.Fatalf("unmarshalling failed: %v", err)
	}
	if got.Name != jobReq.Name {
		t.Fatalf("got %s, want %s", got.Name, jobReq.Name)
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

	var got scheduler.SchedulerStats

	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Errorf("failed to unmarshal stats response: %v", err)
	}

	if got != fake.stats {
		t.Errorf("got %v, want %v", got, fake.stats)
	}
}

func TestHandleGetJobById(t *testing.T) {

	fake := &mockScheduler{}
	exptected := domain.Job{
		ID:   42,
		Name: "test name",
	}
	fake.jobToReturn = exptected
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
	var got domain.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Errorf("failed to unmarshal job response: %v", err)
	}
	if fake.requestId != got.ID {
		t.Errorf("got %d, want %d", got.ID, fake.requestId)
	}
	if !reflect.DeepEqual(got, fake.jobToReturn) {
		t.Errorf("got %+v, want %+v", got, exptected)
	}
}
