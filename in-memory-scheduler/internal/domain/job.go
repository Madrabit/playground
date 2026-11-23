package domain

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
)

const MaxJobs = 1000

type Metadata struct {
	CreatedAt int64
	UpdatedAt int64
}

type Job struct {
	ID          int                 // 8
	Complexity  int                 // 8
	Name        string              // 16
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

func (j Job) Clone() Job {
	j.Tags = maps.Clone(j.Tags)
	j.Attrs = maps.Clone(j.Attrs)
	j.SomeSlice = slices.Clone(j.SomeSlice)
	return j
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

// String returns the stable JSON representation of a job status.
func (s JobStatus) String() string {
	switch s {
	case Pending:
		return "pending"
	case Running:
		return "running"
	case Failed:
		return "failed"
	case Done:
		return "done"
	case Cancelled:
		return "cancelled"
	case Stopped:
		return "stopped"
	default:
		return ""
	}
}

// MarshalJSON encodes known statuses by name instead of exposing enum numbers.
func (s JobStatus) MarshalJSON() ([]byte, error) {
	name := s.String()
	if name == "" {
		return nil, fmt.Errorf("marshal job status %d: %w", s, ErrUnknownJobStatus)
	}
	return json.Marshal(name)
}

// UnmarshalJSON decodes only stable, known status names.
func (s *JobStatus) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return fmt.Errorf("unmarshal job status: expected string: %w", err)
	}
	status, err := ParseJobStatus(name)
	if err != nil {
		return err
	}
	*s = status
	return nil
}

// ParseJobStatus converts a public status name to the internal enum.
func ParseJobStatus(name string) (JobStatus, error) {
	for _, status := range [...]JobStatus{Pending, Running, Failed, Done, Cancelled, Stopped} {
		if status.String() == name {
			return status, nil
		}
	}
	return 0, fmt.Errorf("unknown job status %q: %w", name, ErrUnknownJobStatus)
}

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
