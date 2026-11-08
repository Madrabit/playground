package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"playground/in-memory-scheduler/internal/app"
	"syscall"
	"time"
)

func main() {

	application := app.NewApp()
	err := application.Start()
	if err != nil {
		log.Fatalf("error starting app: %v", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	application.Stop(shutdownCtx)
}
