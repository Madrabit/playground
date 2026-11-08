package scheduler

import (
	"context"
	"errors"
	"fmt"
	"playground/in-memory-scheduler/internal/domain"
)

func LoggingMiddleware(next Processor) Processor {
	return ProcessFunc(func(ctx context.Context, job domain.Job) (domain.JobStatus, error) {
		fmt.Println("logging before")
		jobStatus, err := next.Process(ctx, job)
		fmt.Println("logging after")
		return jobStatus, err
	})
}

type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string {
	return e.Err.Error()
}

func (e RetryableError) Unwrap() error {
	return e.Err
}

func RetryMiddleware(attempts int) Middleware {
	return func(next Processor) Processor {
		return ProcessFunc(func(ctx context.Context, job domain.Job) (domain.JobStatus, error) {
			var state domain.JobStatus
			var err error
			var retryErr *RetryableError
			for i := 0; i < attempts; i++ {
				state, err := next.Process(ctx, job)
				if errors.As(err, &retryErr) {
					continue
				}
				if err != nil {
					return state, err
				}
			}
			return state, err
		})
	}
}

type Middleware func(processor Processor) Processor

func Use(middlewares ...Middleware) Middleware {
	return func(processor Processor) Processor {
		for i := len(middlewares) - 1; i >= 0; i-- {
			processor = middlewares[i](processor)
		}
		return processor
	}
}
