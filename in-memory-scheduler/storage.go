package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type StoreAdmin interface {
	Reset()
}

type TagStore interface {
	AddTag(id int, tag string)
}

type StoreWriter interface {
	Save(job Job) error
	Update(job Job) error
	Rename(id int, newName string) error
	RemoveByID(id int)
}

type StoreReader interface {
	GetByID(id int) (Job, error)
	ListIDs() []int
}

type StoreDebug interface {
	Dump() string
	DumpTags() string
}

type Storage struct {
	jobs map[int]Job
	mu   sync.RWMutex
}

func NewStorage() *Storage {
	jobs := make(map[int]Job, maxJobs)
	return &Storage{jobs, sync.RWMutex{}}
}

func (s *Storage) Save(job Job) error {
	if _, ok := s.jobs[job.ID]; ok {
		return ErrDuplicateJob
	}
	s.jobs[job.ID] = job
	return nil
}

func (s *Storage) Update(job Job) error {
	_, ok := s.jobs[job.ID]
	if !ok {
		return fmt.Errorf("job %d does not exist %w", job.ID, ErrJobNotFound)
	}
	s.jobs[job.ID] = job
	return nil
}

func (s Storage) Dump() string {
	var br strings.Builder
	for _, j := range s.jobs {
		br.WriteString(strconv.Itoa(j.ID))
		br.WriteString(":")
		br.WriteString(j.Name)
		br.WriteString("\n")
	}
	return br.String()
}

func (s *Storage) GetByID(id int) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if ok {
		return job, nil
	}
	return Job{}, ErrJobNotFound
}

func (s *Storage) ListIDs() []int {
	ids := make([]int, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	return ids
}

func (s *Storage) Reset() {
	s.jobs = make(map[int]Job)
}

func (s *Storage) Rename(id int, newName string) error {
	job, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("no such job by id: %w", ErrJobNotFound)

	}
	job.Name = newName
	s.jobs[id] = job
	return nil
}

func (s *Storage) RemoveByID(id int) {
	delete(s.jobs, id)
}

func (s *Storage) DumpTags() string {
	tags := make([]string, 0, len(s.jobs))
	dedupe := make(map[string]struct{})
	for _, v := range s.jobs {
		for tag := range v.Tags {
			if _, ok := dedupe[tag]; !ok {
				dedupe[tag] = struct{}{}
				tags = append(tags, tag)
			}
		}
	}
	sort.Strings(tags)
	return strings.Join(tags, ",")
}

func (s *Storage) AddTag(id int, tag string) {
	job, ok := s.jobs[id]
	if ok {
		job.Tags[tag] = struct{}{}
		s.jobs[id] = job
	}
}
