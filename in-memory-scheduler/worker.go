package main

import (
	"context"
	"fmt"
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
	ctx       context.Context
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

func NewWorker(ctx context.Context, ID int, results chan Results, processor Processor, heartBeat chan<- HeartBeat, wg *sync.WaitGroup) *Worker {
	return &Worker{
		ctx:       ctx,
		ID:        ID,
		stop:      make(chan struct{}),
		jobs:      make(chan Job),
		results:   results,
		processor: processor,
		heartBeat: heartBeat,
		wg:        wg,
	}
}

func (w *Worker) Loop() {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
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
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(1 * time.Second)
			resChan := make(chan Results, 1)
			cancel := make(chan struct{})
			w.state.Store(Busy)
			go func() {
				state, err := w.Process(w.ctx, job)
				resChan <- Results{
					job.ID,
					state,
					err,
				}
			}()
			select {
			case <-w.stop:
				close(cancel)
				w.wg.Done()
				fmt.Println("cancel in worker")
				return
			case res := <-resChan:
				w.wg.Done()
				w.results <- res
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
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

//go:noinline
func (w *Worker) Process(ctx context.Context, j Job) (JobStatus, error) {
	process, err := w.processor.Process(ctx, j)
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

func (w *Worker) CheckHealth() {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-timer.C:
			w.heartBeat <- HeartBeat{w.ID, time.Now()}

		}
		timer.Reset(1 * time.Second)
	}
}

func (w *Worker) Start() {
	w.once.Do(func() {
		go w.CheckHealth()
		go w.Loop()
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
