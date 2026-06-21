package main

import (
	"fmt"
	"testing"
	"time"
)

func Benchmark_Interface(b *testing.B) {
	var p Processor = &PrintProcessor{}

	job := Job{ID: 1}

	b.ResetTimer()
	cancel := make(chan struct{})
	for i := 0; i < b.N; i++ {
		p.Process(job, cancel)
	}
}

func Benchmark_DirectCall(b *testing.B) {
	var p PrintProcessor = PrintProcessor{}

	job := Job{ID: 1}

	b.ResetTimer()
	cancel := make(chan struct{})
	for i := 0; i < b.N; i++ {
		p.Process(job, cancel)
	}
}

func TestLongProcess(t *testing.T) {
	app := NewApp()
	Register("longProcess", NewLongProcessor)
	processor, err := CreateProcessor("longProcess")
	if err != nil {
		fmt.Println(err)
	}
	app.processor = processor
	app.Start()
	// Создаём джобу
	job := Job{
		ID:    1,
		State: Pending,
	}

	// Кладём её в storage
	err = app.scheduler.writer.Save(job)
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	// Отправляем job в очередь
	app.scheduler.Add(job)

	// Ждём 100ms и вызываем Stop()
	time.Sleep(1000 * time.Millisecond)
	app.Stop()

	// Читаем job из storage после Stop()
	updated, err := app.scheduler.reader.GetByID(job.ID)
	if err != nil {
		t.Fatalf("failed to read job: %v", err)
	}

	if updated.State != Cancelled {
		t.Fatalf("expected job to be Cancelled, got %v", updated.State)
	}
}
