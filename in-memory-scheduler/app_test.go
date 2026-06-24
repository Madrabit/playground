package main

import (
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
