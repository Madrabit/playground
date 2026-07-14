package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type FakeProcessor struct{}

func NewFakeProcessor() Processor {
	return &FakeProcessor{}
}

func (f FakeProcessor) Process(ctx context.Context, job Job) (JobStatus, error) {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Cancelled, nil
	case <-timer.C:
		fmt.Printf("Proccess ID: %d", job.ID)
		return Failed, fmt.Errorf("job: %d failed %w", job.ID, ErrFailedProcess)
	}
}

func TestWorkerProcessesJob(t *testing.T) {
	results := make(chan Results, 1)
	heartBeat := make(chan<- HeartBeat, 1)
	Register("fake", NewFakeProcessor)
	processor, err := CreateProcessor("fake")
	if err != nil {
		t.Errorf("Error creating processor: %v", err)
	}

	var wg sync.WaitGroup
	worker := NewWorker(1, results, processor, heartBeat, &wg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)
	job := Job{
		ID:   1,
		Name: "Test",
	}
	worker.Enqueue(job)
	select {
	case res := <-results:
		if res.JobID != 1 {
			t.Fatalf("expected JobID=1, got %d", res.JobID)
		}
		if res.State != Failed { // FakeProcessor всегда возвращает Failed по таймеру
			t.Fatalf("expected status Failed, got %v", res.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not return result in time")
	}
}

func TestWorkerCancelsJob(t *testing.T) {
	results := make(chan Results, 1)
	heartBeat := make(chan HeartBeat, 1)

	Register("long", NewLongProcessor)
	processor, _ := CreateProcessor("long")

	var wg sync.WaitGroup
	worker := NewWorker(1, results, processor, heartBeat, &wg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)

	job := Job{ID: 1}

	worker.Enqueue(job)

	// Останавливаем воркер сразу → cancel должен сработать
	worker.Stop()

	select {
	case res := <-results:
		if res.State != Cancelled {
			t.Fatalf("expected Cancelled, got %v", res.State)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not cancel job in time")
	}
}
