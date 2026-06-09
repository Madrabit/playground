package main

import (
	"fmt"
)

const maxJobs = 1000

type Metadata struct {
	CreatedAt int64
	UpdatedAt int64
}

type Job struct {
	ID          int                 // 8
	Complexity  int                 // 8
	Name        string              `json:"name"` // 16
	Description string              // 16
	Tags        map[string]struct{} // 16
	SomeSlice   []string
	Attrs       map[string]string
	State       JobStatus
}

func NewJob(ID int, Name string) *Job {
	return &Job{
		ID:    ID,
		Name:  Name,
		Tags:  make(map[string]struct{}),
		Attrs: make(map[string]string),
	}
}

func (j Job) FirstLetter() rune {
	if j.Name != "" {
		for _, v := range j.Name {
			return v
		}
	}
	return 0
}

func (j Job) PrintRunes() {
	for i, v := range j.Name {
		fmt.Printf("%d,%c\n", i, v)
	}
}

func (j *Job) SetTag(tag string) error {
	j.Tags[tag] = struct{}{}
	return nil
}

func (j *Job) HasSameTags(other Job) (result bool) {
	for k := range j.Tags {
		if _, ok := other.Tags[k]; ok {
			return true
		}
	}
	return false
}

func (j *Job) AddTag(tag string) bool {
	_, ok := j.Tags[tag]
	if ok {
		return false
	}
	j.Tags[tag] = struct{}{}
	return true
}
func (j *Job) RemoveTag(tag string) {
	delete(j.Tags, tag)
}

type JobStatus int

const (
	Pending JobStatus = iota
	Running
	Failed
	Done
	Cancelled
	Stopped
)

func (j *Job) Transition(to JobStatus) error {
	switch j.State {
	case Pending:
		switch to {
		case Running, Cancelled:
			j.State = to
			return nil
		default:
			return ErrInvalidTransition
		}
	case Running:
		switch to {
		case Done, Failed, Cancelled:
			j.State = to
			return nil
		default:
			return ErrInvalidTransition
		}
	case Done, Failed, Cancelled:
		return ErrInvalidTransition
	default:
		return ErrInvalidTransition
	}
}
