package main

import (
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
	ID                int
	stop              chan struct{}
	jobs              chan Job
	results           chan<- Results
	processor         Processor
	wg                sync.WaitGroup
	heartBeat         chan<- HeartBeat
	once              sync.Once
	state             atomic.Int32
	validationRequest chan ValidationRequest
}

type Processor interface {
	Process(job Job, cancel <-chan struct{}) (JobStatus, error)
}

func NewWorker(ID int, results chan Results, processor Processor, heartBeat chan<- HeartBeat) *Worker {
	return &Worker{
		ID:        ID,
		stop:      make(chan struct{}),
		jobs:      make(chan Job),
		results:   results,
		processor: processor,
		heartBeat: heartBeat,
	}
}

func (w *Worker) Loop() {
	defer func() {
		w.state.Store(StoppedWorker)
	}()
	for {
		w.state.Store(Idle)
		select {
		case <-w.stop:
			return
		case job := <-w.jobs:
			resChan := make(chan Results, 1)
			cancel := make(chan struct{})
			w.state.Store(Busy)
			go func() {
				state, err := w.Process(job, cancel)
				select {
				case <-cancel:
					return
				case resChan <- Results{
					job.ID,
					state,
					err,
				}:
				}
			}()
			select {
			case <-w.stop:
				close(cancel)
				return
			case res := <-resChan:
				w.results <- res
			case <-time.After(1 * time.Second):
				w.results <- Results{
					job.ID,
					Failed,
					fmt.Errorf("timeout exceeded waiting for job %d", job.ID),
				}
			}
		}
	}

}

//go:noinline
func (w *Worker) Process(j Job, cancel <-chan struct{}) (JobStatus, error) {
	process, err := w.processor.Process(j, cancel)
	return process, err
}

func (w *Worker) Enqueue(job Job) {
	job.State = Running
	w.jobs <- job
}

func (w *Worker) CheckHealth() {
	for {
		select {
		case <-w.stop:
			return
		case <-time.After(1 * time.Second):
			w.heartBeat <- HeartBeat{w.ID, time.Now()}
		}
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
