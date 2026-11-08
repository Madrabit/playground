package storage

import (
	"errors"
)

var (
	ErrDuplicateJob = errors.New("job already exists")
	ErrJobNotFound  = errors.New("job not found")
)
