package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestIntegration_SchedulerEndToEnd(t *testing.T) {
	// --- Каналы ---
	validationRequest := make(chan ValidationRequest, 10)
	results := []chan Results{make(chan Results, 10)}
	heartBeat := make(chan HeartBeat, 10)

	// --- Валидатор ---
	validator := NewValidator(validationRequest)
	go validator.Loop() // запускаем реальный валидатор

	// --- Очереди ---
	highQueue := NewJobsQueue()
	normalQueue := NewJobsQueue()

	// --- Storage (in-memory) ---
	storage := NewStorage() // твой реальный in-memory storage
	job := Job{ID: 1, Name: "TestJob"}
	err := storage.Save(job)
	if err != nil {
		return
	} // кладём job в storage

	// --- Processor (фейковый, но настоящий) ---
	Register("fake", func() Processor { return &FakeProcessor{} })
	processor, _ := CreateProcessor("fake")

	var wg sync.WaitGroup
	// --- Worker ---
	worker := NewWorker(1, results[0], processor, heartBeat, &wg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)

	// --- Scheduler ---
	scheduler := NewScheduler(
		validator,
		normalQueue,
		highQueue,
		validationRequest,
		results,
		heartBeat,
	)
	scheduler.reader = storage
	scheduler.workers = []WorkerI{worker}

	go scheduler.DispatchLoop()

	// --- Кладём job в очередь ---
	highQueue.Push(1)

	// --- Ждём результат ---
	select {
	case res := <-results[0]:
		if res.JobID != 1 {
			t.Fatalf("expected job ID=1, got %d", res.JobID)
		}
		// FakeProcessor возвращает Failed
		if res.State != Failed {
			t.Fatalf("expected Failed, got %v", res.State)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	// --- Останавливаем всё ---
	scheduler.Stop()
	worker.Stop()
}

func TestMergeResults(t *testing.T) {
	// два входных канала
	ch1 := make(chan Results, 1)
	ch2 := make(chan Results, 1)

	// mergedChan куда всё собирается
	merged := make(chan Results, 2)

	s := &Scheduler{
		results:    []chan Results{ch1, ch2},
		mergedChan: merged,
	}

	// запускаем merge
	s.MergeResults()

	// отправляем данные
	ch1 <- Results{JobID: 1}
	ch2 <- Results{JobID: 2}

	// закрываем входные каналы, чтобы горутины завершились
	close(ch1)
	close(ch2)

	// собираем результаты
	got := []int{}
	for i := 0; i < 2; i++ {
		r := <-merged
		got = append(got, r.JobID)
	}

	// проверяем, что оба ID пришли
	want := map[int]bool{1: true, 2: true}
	for _, id := range got {
		if !want[id] {
			t.Fatalf("unexpected id %d", id)
		}
	}
}
