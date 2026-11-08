package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"playground/in-memory-scheduler/internal/httpapi"
	scheduler "playground/in-memory-scheduler/internal/scheduler"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type App struct {
	ctx       context.Context
	cancel    context.CancelFunc
	validator *scheduler.Validator
	scheduler *scheduler.Scheduler
	workers   []scheduler.WorkerI
	processor scheduler.Processor

	httpServer *http.Server
	ready      atomic.Bool
	goroutines atomic.Int64
	cpus       int
	wg         sync.WaitGroup
	queue      *scheduler.JobsQueue
	heartBeat  chan scheduler.HeartBeat
	results    []chan scheduler.Results
}

func NewApp() *App {
	ctx, cancel := context.WithCancel(context.Background())
	validationRequest := make(chan scheduler.ValidationRequest, 1024)
	results := make([]chan scheduler.Results, 0, 1024)
	heartBeat := make(chan scheduler.HeartBeat, 1024)
	validator := scheduler.NewValidator(validationRequest)
	scheduler.Register("print", scheduler.NewPrintProcessor)
	processor, err := scheduler.CreateProcessor("print")
	if err != nil {
		log.Printf("error creating processor %v\n", err)
	}
	normalQueue := scheduler.NewJobsQueue() // нужно ли очередь отключать через контекст, она сейчас отключается через cond
	highQueue := scheduler.NewJobsQueue()
	cpus := runtime.NumCPU()
	scheduler := scheduler.NewScheduler(validator, normalQueue, highQueue, validationRequest, results, heartBeat)
	app := &App{
		ctx:       ctx,
		cancel:    cancel,
		validator: validator,
		scheduler: scheduler,
		cpus:      cpus,
		wg:        sync.WaitGroup{},
		results:   results,
		processor: processor,
		heartBeat: heartBeat,
	}
	app.httpServer = &http.Server{
		Addr:              ":8080",
		Handler:           httpapi.NewHttpHandler(app.isReady, scheduler),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return app
}

func (app *App) Start() error {
	listener, err := net.Listen("tcp", app.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	app.startWorkers(app.cpus)
	app.scheduler.Workers = app.workers
	app.scheduler.Start()
	app.startValidator()

	app.Ready(true)

	go func() {
		err = app.httpServer.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()
	return nil
}

func (app *App) startValidator() {
	go func() {
		app.validator.Loop()
	}()
}

func (app *App) startWorkers(cpus int) {
	n := cpus * 4
	//n := 2
	app.workers = make([]scheduler.WorkerI, app.cpus*4)
	processor := scheduler.Use(
		scheduler.LoggingMiddleware,
		scheduler.RetryMiddleware(3),
	)(app.processor)
	for i := 0; i < n; i++ {
		resultChan := make(chan scheduler.Results)
		w := scheduler.NewWorker(i, resultChan, processor, app.heartBeat, &app.scheduler.JobsWg)
		app.workers[i] = w
		app.results = append(app.results, resultChan)
		app.scheduler.Results = append(app.scheduler.Results, resultChan)
	}
	for _, w := range app.workers {
		app.StartWorker(w)
	}
}

func (app *App) StartWorker(w scheduler.WorkerI) {
	app.wg.Add(1)
	app.goroutines.Add(1)
	go func() {
		defer app.goroutines.Add(-1)
		defer app.wg.Done()
		w.Start(app.ctx)
	}()
}

func (app *App) StopWorker(w scheduler.WorkerI) {
	w.Stop()
}

func (app *App) StopValidator() {
	app.validator.Stop()
}

func (app *App) Stop(ctx context.Context) {
	app.Ready(false)
	httpErr := app.httpServer.Shutdown(ctx)
	if httpErr != nil {
		log.Printf("HTTP graceful shutdown failed: %v", httpErr)

		if closeErr := app.httpServer.Close(); closeErr != nil {
			log.Printf("HTTP force close failed: %v", closeErr)
		}
	}
	app.scheduler.StopDispatch()
	app.scheduler.JobsWg.Wait()
	for _, w := range app.workers {
		app.StopWorker(w)
	}
	for _, ch := range app.scheduler.Results {
		close(ch)
	}
	app.scheduler.MergeWg.Wait()
	close(app.scheduler.MergedChan)
	// защитный дамп перед ожиданием — если Stop зависнет, дамп уже будет в логах
	go func() {
		// через 5s после вызова Stop сделаем дамп, если Wait всё ещё висит
		time.Sleep(5 * time.Second)
		fmt.Println("Stop() seems stuck — dumping goroutines")
		scheduler.DumpStacks()
	}()
	//app.scheduler.wg.Wait()
	app.StopValidator()
	app.scheduler.Stop()
	app.wg.Wait()
}

func (app *App) CloseQueue() {
	app.queue.Close()
}

func (a *App) Ready(ready bool) {
	a.ready.Store(ready)
}

func (a *App) isReady() bool {
	return a.ready.Load()
}
