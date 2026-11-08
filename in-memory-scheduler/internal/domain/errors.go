package domain

import (
	"errors"
)

var (
	ErrInvalidTransition = errors.New("invalid transition")
	ErrJobNotFound       = errors.New("job not found")
)
