package main

import (
	"fmt"
	"sync"
	"time"
)

type Worker struct {
	ID        int
	stop      chan struct{}
	jobs      chan Job
	results   chan<- Results
	processor Processor
	wg        sync.WaitGroup
	heartBeat chan<- HeartBeat
}

type Processor interface {
	Process(job Job) (JobStatus, error)
}

func NewWorker(ID int, results chan<- Results, processor Processor, heartBeat chan<- HeartBeat) *Worker {
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
	go w.CheckHealth()
	for {
		select {
		case <-w.stop:
			return
		case job := <-w.jobs:
			resChan := make(chan Results, 1)
			cancel := make(chan struct{})
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
	process, err := w.processor.Process(j)
	return process, err
}

func (w *Worker) Enqueue(job Job) {
	w.jobs <- job
}

type HeartBeat struct {
	jobId    int
	LastSeen time.Time
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
