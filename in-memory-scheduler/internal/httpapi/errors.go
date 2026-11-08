package httpapi

import (
	"errors"
)

var (
	ErrInvalidTransition = errors.New("invalid transition")
	ErrJobNotFound       = errors.New("job not found")
	ErrDuplicateJob      = errors.New("job already exists")
	ErrQueueFull         = errors.New("queue is full")
	ErrValidation        = errors.New("validation error")
	ErrFailedProcess     = errors.New("failed process")
	ErrProcessNotFound   = errors.New("process not found")
	ErrProcessAborted    = errors.New("process aborted")
)
