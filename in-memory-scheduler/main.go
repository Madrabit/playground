package main

import (
	"errors"
	"fmt"
	"log"
	"runtime"
)

func main() {
	cpus := runtime.NumCPU()
	Register("print", NewPrintProcessor)
	processor, err := CreateProcessor("print")
	if err != nil {
		log.Printf("error creating processor %v\n", err)
	}
	scheduler := NewScheduler(processor, cpus)
	err = scheduler.Add(Job{
		ID:   1,
		Name: "Test",
	})
	if errors.Is(err, ErrDuplicateJob) {
		fmt.Println("Duplicate job")
	}
	job, err := scheduler.reader.GetByID(1)
	if err == nil {
		scheduler.validator.Validate(job)
	}
	scheduler.ValidateAsync()

	err = BadError()
	if err != nil {
		fmt.Println("Bad error not nil:", err)
	}

	err = scheduler.AddAndLog(job)
	if err != nil {
		log.Printf("error adding job: %v", err)
	}
	_, err = scheduler.GetById(33)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			fmt.Println("Job not found")
		}
	}
	status := ToHTTPStatus(err)
	fmt.Println("status:", status)
}
