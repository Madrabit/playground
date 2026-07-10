package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Busy int32 = iota
	Idle
	StoppedWorker
)

type Worker struct {
	ID        int
	stop      chan struct{}
	jobs      chan Job
	results   chan<- Results
	processor Processor
	heartBeat chan<- HeartBeat
	once      sync.Once
	state     atomic.Int32
	wg        *sync.WaitGroup
}

type Processor interface {
	Process(ctx context.Context, job Job) (JobStatus, error)
}

func NewWorker(ID int, results chan Results, processor Processor, heartBeat chan<- HeartBeat, wg *sync.WaitGroup) *Worker {
	return &Worker{
		ID:        ID,
		stop:      make(chan struct{}),
		jobs:      make(chan Job),
		results:   results,
		processor: processor,
		heartBeat: heartBeat,
		wg:        wg,
	}
}

func (w *Worker) Loop(ctx context.Context) {
	timer := time.NewTimer(0)
	stopTime(timer)
	defer func() {
		timer.Stop()
		w.state.Store(StoppedWorker)
	}()
	for {
		w.state.Store(Idle)
		select {
		case <-w.stop:
			return
		case job, ok := <-w.jobs:
			if !ok {
				return
			}
			fmt.Println("Worker got job", job.ID)
			stopTime(timer)
			timer.Reset(1 * time.Second)
			resChan := make(chan Results, 1)
			w.state.Store(Busy)
			go func() {
				state, err := w.Process(ctx, job)
				resChan <- Results{
					job.ID,
					state,
					err,
				}
			}()
			select {
			case <-w.stop:
				w.wg.Done()
				fmt.Println("cancel in worker")
				return
			case res := <-resChan:
				w.wg.Done()
				res = normalizeResult(res)
				w.results <- res
				stopTime(timer)
			case <-timer.C:
				select {
				case <-w.stop:
					w.wg.Done()
					return
				default:
					w.results <- Results{
						job.ID,
						Failed,
						fmt.Errorf("timeout exceeded waiting for job %d", job.ID),
					}
					w.wg.Done()
				}
			}
		}
	}
}

func normalizeResult(res Results) Results {
	switch {
	case errors.Is(res.Error, context.Canceled):
		res.State = Cancelled
	// таймаут — ошибка SLA
	case errors.Is(res.Error, context.DeadlineExceeded):
		log.Println("timeout:", res.JobID)
		res.State = Failed
	case res.Error != nil:
		log.Println("processor error:", res.Error)
		res.State = Failed
	}
	return res
}

func stopTime(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

// Отмена процесса по таймауту 1 секунда
//
//go:noinline
func (w *Worker) Process(ctx context.Context, j Job) (JobStatus, error) {
	const jobTimeout = time.Second * 1
	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()
	process, err := w.processor.Process(jobCtx, j)
	return process, err
}

func (w *Worker) Enqueue(job Job) {
	fmt.Println("Enqueue", job.ID)
	newJob := job
	newJob.State = Running
	select {
	case <-w.stop:
		return
	case w.jobs <- newJob:
		return
	}
}

func (w *Worker) Stop() {
	close(w.stop)
}

func (w *Worker) CheckHealth(ctx context.Context) {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			w.heartBeat <- HeartBeat{w.ID, time.Now()}

		}
		timer.Reset(1 * time.Second)
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.once.Do(func() {
		go w.CheckHealth(ctx)
		go w.Loop(ctx)
	})
}

func (w *Worker) IsBusy() bool {
	return w.state.Load() == Busy
}

func (w *Worker) IsIdle() bool {
	return w.state.Load() == Idle
}

func (w *Worker) IsStopped() bool {
	return w.state.Load() == StoppedWorker
}
