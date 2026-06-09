package main

import (
	"errors"
	"fmt"
)

func LoggingMiddleware(next Processor) Processor {
	return ProcessFunc(func(job Job) (JobStatus, error) {
		fmt.Println("logging before")
		jobStatus, err := next.Process(job)
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
		return ProcessFunc(func(job Job) (JobStatus, error) {
			var state JobStatus
			var err error
			var retryErr *RetryableError
			for i := 0; i < attempts; i++ {
				state, err := next.Process(job)
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
