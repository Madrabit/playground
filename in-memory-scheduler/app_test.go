package main

import (
	"os"
	"runtime/trace"
	"testing"
	"time"
)

func TestIntegration_Shutdown(t *testing.T) {
	app := NewApp()

	// стартуем всю систему
	app.Start()

	// кладём несколько job
	for i := 1; i <= 5; i++ {
		app.scheduler.Add(Job{ID: i, Name: "Test"})
	}

	// даём системе поработать
	time.Sleep(500 * time.Millisecond)

	// останавливаем всю систему
	app.Stop()

	// ждём завершения всех горутин
	time.Sleep(100 * time.Millisecond)

	// проверяем, что нет утечек
	if app.goroutines.Load() != 0 {
		t.Fatalf("expected no goroutines, got %d", app.goroutines.Load())
	}
}

func TestFatJobs(t *testing.T) {
	f, _ := os.Create("trace.out")
	trace.Start(f)
	defer trace.Stop()
	app := NewApp()
	app.Start()

	const totalJobs = 10000

	accepted := 0
	rejected := 0

	// отправляем много задач
	for i := 1; i <= totalJobs; i++ {
		err := app.scheduler.Add(Job{ID: i, Name: "Test"})
		if err == ErrQueueFull {
			rejected++
		} else if err != nil {
			t.Fatalf("unexpected error: %v", err)
		} else {
			accepted++
		}
	}

	// даём системе поработать
	time.Sleep(2 * time.Second)

	// останавливаем
	app.Stop()

	// считаем обработанные задачи
	processed := 0
	for i := 1; i <= totalJobs; i++ {
		j, err := app.scheduler.reader.GetByID(i)
		if err == nil && j.State != Pending {
			processed++
		}
	}

	t.Logf("Total jobs: %d", totalJobs)
	t.Logf("Accepted: %d", accepted)
	t.Logf("Rejected: %d", rejected)
	t.Logf("Processed: %d", processed)

	if accepted == 0 {
		t.Fatal("no jobs were accepted — backpressure too strict or system broken")
	}
	if processed == 0 {
		t.Fatal("no jobs were processed — workers not running?")
	}
}
