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
	workers    []WorkerI
	processor  Processor
	goroutines atomic.Int64
	cpus       int
	wg         sync.WaitGroup
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
		heartBeat: heartBeat,
	}
}

type Queue interface {
	Len() int
	Push(id int) error
	Pop() (int, bool)
	Close()
}

type WorkerI interface {
	Loop()
	Process(j Job, cancel <-chan struct{}) (JobStatus, error)
	Enqueue(job Job)
	CheckHealth()
	Start()
	IsBusy() bool
	IsIdle() bool
	IsStopped() bool
	Stop()
}

func (app *App) Start() {
	app.startWorkers(app.cpus)
	app.scheduler.workers = app.workers
	app.scheduler.Start()
	app.startValidator()
}

func (app *App) startValidator() {
	go func() {
		app.validator.Loop()
	}()
}

func (app *App) startWorkers(cpus int) {
	n := cpus * 4
	app.workers = make([]WorkerI, app.cpus*4)
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

func (app *App) StartWorker(w WorkerI) {
	app.wg.Add(1)
	app.goroutines.Add(1)
	go func() {
		defer app.goroutines.Add(-1)
		defer app.wg.Done()
		w.Start()
	}()
}

func (app *App) StopWorker(w WorkerI) {
	w.Stop()
}

func (app *App) StopValidator() {
	app.validator.Stop()
}

func (app *App) Stop() {
	for _, w := range app.workers {
		app.StopWorker(w)
	}
	app.wg.Wait()
	app.StopValidator()
	app.scheduler.Stop()
}

func (app *App) CloseQueue() {
	app.queue.Close()
}

type HeartBeat struct {
	jobId    int
	LastSeen time.Time
}
