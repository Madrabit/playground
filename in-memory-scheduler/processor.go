package main

import (
	"fmt"
	"time"
)

type PrintProcessor struct{}

func NewPrintProcessor() Processor {
	return &PrintProcessor{}
}

func (print *PrintProcessor) Process(job Job, cancel <-chan struct{}) (JobStatus, error) {
	//fmt.Printf("pid:%d", job.ID)
	select {
	case <-cancel:
		return Cancelled, nil
	case <-time.After(time.Second):
		x := job.ID + 1
		if x%10 == 0 {
			return Done, nil
		}
		return Pending, nil
	}
}

type SleepProcessor struct {
	Duration time.Duration
}

func NewSleepProcessor() Processor {
	return &SleepProcessor{}
}

func (p *SleepProcessor) Process(job Job, cancel <-chan struct{}) (JobStatus, error) {
	select {
	case <-cancel:
		return Cancelled, nil
	case <-time.After(time.Second):
		time.Sleep(1 * time.Second)
		return Pending, nil
	}
}

type FailProcessor struct{}

func NewFailProcessor() Processor { return &FailProcessor{} }

func (p *FailProcessor) Process(job Job, cancel <-chan struct{}) (JobStatus, error) {
	select {
	case <-cancel:
		return Cancelled, nil
	case <-time.After(time.Second):
		fmt.Printf("Proccess ID: %d", job.ID)
		return Failed, fmt.Errorf("job: %d failed %w", job.ID, ErrFailedProcess)
	}
}

type ProcessFunc func(job Job) (JobStatus, error)

func (f ProcessFunc) Process(job Job) (JobStatus, error) {
	return f(job)
}

var registry = make(map[string]ProcessFactory)

type ProcessFactory func() Processor

func Register(name string, factory ProcessFactory) {
	registry[name] = factory
}

func CreateProcessor(name string) (Processor, error) {
	factory, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("no such processor %q %w", name, ErrProcessNotFound)
	}
	return factory(), nil
}

func DebugProcessor(p Processor) {
	switch v := p.(type) {
	case *SleepProcessor:
		fmt.Println("SleepProcessor")
		fmt.Println(v.Duration)
	case *FailProcessor:
		fmt.Println("FailProcessor")
	case *PrintProcessor:
		fmt.Println("PrintProcessor")
	}
}

type HangingProcessor struct {
	stop chan struct{}
}

func NewHangingProcessor() Processor {
	return &HangingProcessor{stop: make(chan struct{})}
}

func (h *HangingProcessor) Process(job Job, cancel <-chan struct{}) (JobStatus, error) {
	select {
	case <-cancel:
		return Cancelled, nil
	case <-h.stop:
		return Done, nil
	default:
		return Pending, nil
	}
}
