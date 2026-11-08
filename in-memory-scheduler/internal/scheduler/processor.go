package scheduler

import (
	"context"
	"fmt"
	"playground/in-memory-scheduler/internal/domain"
	"time"
)

type PrintProcessor struct{}

func NewPrintProcessor() Processor {
	return &PrintProcessor{}
}

func (print *PrintProcessor) Process(ctx context.Context, job domain.Job) (domain.JobStatus, error) {
	//fmt.Printf("pid:%d", job.ID)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return domain.Cancelled, nil
	case <-timer.C:
		x := job.ID + 1
		if x%10 == 0 {
			return domain.Done, nil
		}
		return domain.Pending, nil
	}
}

type SleepProcessor struct {
	Duration time.Duration
}

func NewSleepProcessor() Processor {
	return &SleepProcessor{}
}

func (p *SleepProcessor) Process(ctx context.Context, _ domain.Job) (domain.JobStatus, error) {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return domain.Cancelled, nil
	case <-timer.C:
		return domain.Pending, nil
	}
}

type FailProcessor struct{}

func NewFailProcessor() Processor { return &FailProcessor{} }

func (p *FailProcessor) Process(ctx context.Context, job domain.Job) (domain.JobStatus, error) {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return domain.Cancelled, nil
	case <-timer.C:
		fmt.Printf("Proccess ID: %d", job.ID)
		return domain.Failed, fmt.Errorf("job: %d failed %w", job.ID, ErrFailedProcess)
	}
}

type LongProcessor struct{}

func NewLongProcessor() Processor {
	return &LongProcessor{}
}

func (p *LongProcessor) Process(ctx context.Context, job domain.Job) (domain.JobStatus, error) {
	fmt.Println("long process started")
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		fmt.Println("cancelled long process")
		return domain.Cancelled, nil
	case <-timer.C:
		fmt.Printf("Long Proccess ID: %d", job.ID)
		return domain.Pending, nil
	}
}

type ProcessFunc func(ctx context.Context, job domain.Job) (domain.JobStatus, error)

func (f ProcessFunc) Process(ctx context.Context, job domain.Job) (domain.JobStatus, error) {
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

func (h *HangingProcessor) Process(ctx context.Context, job domain.Job) (domain.JobStatus, error) {
	<-ctx.Done()
	return domain.Cancelled, nil
}

type CanceledProcessor struct {
}

func NewCanceledProcessor() Processor {
	return &CanceledProcessor{}
}

func (p *CanceledProcessor) Process(ctx context.Context, _ domain.Job) (domain.JobStatus, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return domain.Cancelled, nil
		case <-ticker.C:
		}
	}

}

type NotCanceledProcessor struct {
}

func NewNotCanceledProcessor() Processor {
	return &NotCanceledProcessor{}
}

func (p *NotCanceledProcessor) Process(ctx context.Context, _ domain.Job) (domain.JobStatus, error) {
	for {
		time.Sleep(1 * time.Second)
	}
}

type PanicProcessor struct {
}

func NewPanicProcessor() Processor {
	return &PanicProcessor{}
}

func (p *PanicProcessor) Process(ctx context.Context, _ domain.Job) (domain.JobStatus, error) {
	panic("boom")
}
