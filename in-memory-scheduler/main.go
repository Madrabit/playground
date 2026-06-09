package main

import (
	"errors"
	"fmt"
	"log"
)

func main() {
	Register("print", NewPrintProcessor)
	processor, err := CreateProcessor("print")
	if err != nil {
		log.Printf("error creating processor %v\n", err)
	}
	scheduler := NewScheduler(processor)
	err = scheduler.Add(Job{
		ID:   1,
		Name: "Test",
	})
	if errors.Is(err, ErrDuplicateJob) {
		fmt.Println("Duplicate job")
	}
	job, err := scheduler.reader.GetByID(1)
	if err == nil {
		err := ValidateJob(job)
		var ve *ValidationError
		if errors.As(err, &ve) {
			fmt.Println("поле:", ve.Field)
			fmt.Println("значение:", ve.Value)
			fmt.Println("причина:", ve.Reason)
		}
	}
	errorbutch := scheduler.ValidateAsync()
	statErr := make(map[string]int)
	for _, err := range errorbutch {
		var ve *ValidationError
		if errors.As(err, &ve) {
			statErr[ve.Field]++
		}
	}

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
