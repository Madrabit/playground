package main

import (
	"context"
	"fmt"
	"time"
)

type PrintProcessor struct{}

func NewPrintProcessor() Processor {
	return &PrintProcessor{}
}

func (print *PrintProcessor) Process(ctx context.Context, job Job) (JobStatus, error) {
	//fmt.Printf("pid:%d", job.ID)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Cancelled, nil
	case <-timer.C:
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

func (p *SleepProcessor) Process(ctx context.Context, _ Job) (JobStatus, error) {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Cancelled, nil
	case <-timer.C:
		return Pending, nil
	}
}

type FailProcessor struct{}

func NewFailProcessor() Processor { return &FailProcessor{} }

func (p *FailProcessor) Process(ctx context.Context, job Job) (JobStatus, error) {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Cancelled, nil
	case <-timer.C:
		fmt.Printf("Proccess ID: %d", job.ID)
		return Failed, fmt.Errorf("job: %d failed %w", job.ID, ErrFailedProcess)
	}
}

type LongProcessor struct{}

func NewLongProcessor() Processor {
	return &LongProcessor{}
}

func (p *LongProcessor) Process(ctx context.Context, job Job) (JobStatus, error) {
	fmt.Println("long process started")
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		fmt.Println("cancelled long process")
		return Cancelled, nil
	case <-timer.C:
		fmt.Printf("Long Proccess ID: %d", job.ID)
		return Pending, nil
	}
}

type ProcessFunc func(ctx context.Context, job Job) (JobStatus, error)

func (f ProcessFunc) Process(ctx context.Context, job Job) (JobStatus, error) {
	return f(ctx, job)
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

func (h *HangingProcessor) Process(ctx context.Context, job Job) (JobStatus, error) {
	<-ctx.Done()
	return Cancelled, nil
}

type CanceledProcessor struct {
}

func NewCanceledProcessor() Processor {
	return &CanceledProcessor{}
}

func (p *CanceledProcessor) Process(ctx context.Context, _ Job) (JobStatus, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return Cancelled, nil
		case <-ticker.C:
		}
	}

}

type NotCanceledProcessor struct {
}

func NewNotCanceledProcessor() Processor {
	return &NotCanceledProcessor{}
}

func (p *NotCanceledProcessor) Process(ctx context.Context, _ Job) (JobStatus, error) {
	for {
		time.Sleep(1 * time.Second)
	}
}

type PanicProcessor struct {
}

func NewPanicProcessor() Processor {
	return &PanicProcessor{}
}

func (p *PanicProcessor) Process(ctx context.Context, _ Job) (JobStatus, error) {
	panic("boom")
}
