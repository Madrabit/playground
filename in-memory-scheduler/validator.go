package main

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type Validator struct {
	request chan ValidationRequest
	jobs    chan Job
	stop    chan struct{}
}

func NewValidator(request chan ValidationRequest) *Validator {
	return &Validator{
		request: request,
		stop:    make(chan struct{}),
	}
}

func (v *Validator) Loop() {
	for {
		select {
		case <-v.stop:
			return
		case j := <-v.request:
			err := v.validateJob(j.job)
			j.reply <- ValidationResult{
				JobID: j.JobID,
				job:   j.job,
				Err:   err,
			}
		}
	}
}

func (v *Validator) Stop() {
	close(v.stop)
}

func (v *Validator) validateJob(j Job) error {
	if j.ID <= 0 {
		return &ValidationError{
			Field:  "ID",
			Value:  j.ID,
			Reason: "must be greater than 0",
		}
	}
	if strings.TrimSpace(j.Name) == "" {
		return &ValidationError{
			Field:  "Name",
			Value:  j.Name,
			Reason: "Invalid Job Name",
		}
	}
	if utf8.RuneCountInString(j.Name) > 32 {
		return &ValidationError{
			Field:  "Name",
			Value:  j.Name,
			Reason: "name exceeds 32 characters",
		}
	}

	if j.Complexity > 10 || j.Complexity <= 0 {
		return &ValidationError{
			Field:  "Complexity",
			Value:  j.Complexity,
			Reason: "must be between 1 and 10",
		}
	}
	if len(j.Tags) > 3 {
		return &ValidationError{
			Field:  "Tags",
			Value:  len(j.Tags),
			Reason: "must be no more than 3 tags",
		}
	}
	for k := range j.Tags {
		if strings.TrimSpace(k) == "" {
			return &ValidationError{
				Field:  "Tags",
				Value:  k,
				Reason: "tag cannot be blank",
			}
		}
	}
	return nil
}

type ValidationError struct {
	Field  string
	Value  any
	Reason string
}

func (v *ValidationError) Error() string {
	return fmt.Sprintf("%s (value=%v): %s", v.Field, v.Value, v.Reason)
}
