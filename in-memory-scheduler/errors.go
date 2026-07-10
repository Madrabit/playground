package main

import (
	"errors"
)

var (
	ErrDuplicateJob      = errors.New("job already exists")
	ErrJobNotFound       = errors.New("job not found")
	ErrQueueFull         = errors.New("queue is full")
	ErrInvalidTransition = errors.New("invalid transition")
	ErrFailedProcess     = errors.New("failed process")
	ErrProcessNotFound   = errors.New("process not found")
	ErrProcessAborted    = errors.New("process aborted")
)
