package main

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type App struct {
	validator  *Validator
	scheduler  *Scheduler
	workers    []*Worker
	processor  Processor
	goroutines atomic.Int64
	cpus       int
	wg         sync.WaitGroup
	stop       chan struct{}
	queue      *JobsQueue
	heartBeat  chan HeartBeat
	results    []chan Results
}

func NewApp() *App {
	validationRequest := make(chan ValidationRequest, 1024)
	results := make([]chan Results, 0, 1024)
	heartBeat := make(chan HeartBeat, 1024)
	validator := NewValidator(validationRequest)
	Register("print", NewPrintProcessor)
	processor, err := CreateProcessor("print")
	if err != nil {
		log.Printf("error creating processor %v\n", err)
	}
	normalQueue := NewJobsQueue()
	highQueue := NewJobsQueue()
	cpus := runtime.NumCPU()
	scheduler := NewScheduler(validator, normalQueue, highQueue, validationRequest, results, heartBeat)
	return &App{
		validator: validator,
		scheduler: scheduler,
		cpus:      cpus,
		wg:        sync.WaitGroup{},
		results:   results,
		processor: processor,
		stop:      make(chan struct{}),
	}
}

func (app *App) Start() {
	app.scheduler.Start()
	app.startValidator()
	app.startWorkers(app.cpus)
}

func (app *App) startValidator() {
	go func() {
		app.validator.Loop()
	}()
}

func (app *App) startWorkers(cpus int) {
	n := cpus * 4
	app.workers = make([]*Worker, app.cpus*4)
	processor := Use(
		LoggingMiddleware,
		RetryMiddleware(3),
	)(app.processor)
	for i := 0; i < n; i++ {
		resultChan := make(chan Results)
		w := NewWorker(i, resultChan, processor, app.heartBeat)
		app.workers[i] = w
		app.results = append(app.results, resultChan)
		app.scheduler.results = append(app.scheduler.results, resultChan)
	}
	for _, w := range app.workers {
		app.StartWorker(w)
	}

}

func (app *App) StartWorker(w *Worker) {
	app.wg.Add(1)
	app.goroutines.Add(1)
	go func() {
		defer app.goroutines.Add(-1)
		defer app.wg.Done()
		w.Start()
	}()
}

func (app *App) StopWorker(w *Worker) {
	close(w.stop)
}

func (app *App) StopValidator() {
	app.validator.Stop()
}

func (app *App) Stop() {
	//close(app.stop)
	for _, w := range app.workers {
		app.StopWorker(w)
	}
	app.StopValidator()
	app.scheduler.Stop()
	app.wg.Wait()
}

func (app *App) CloseQueue() {
	app.queue.Close()
}

type HeartBeat struct {
	jobId    int
	LastSeen time.Time
}
