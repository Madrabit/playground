package main

import "fmt"

type JobError struct {
	JobId int
}

func (j *JobError) Error() string {
	return fmt.Sprintf("Job #%d failed", j.JobId)
}

func BadError() error {
	var e *JobError = nil
	return e
}
