package main

import (
	"fmt"
	"io"
	"slices"
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
	/*
		для GetById из забора мапы jobs потому что шарятся данные
	*/
	mu sync.RWMutex
}

func NewStorage() *Storage {
	jobs := make(map[int]Job, maxJobs)
	return &Storage{jobs, sync.RWMutex{}}
}

func (s *Storage) Save(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.ID]; ok {
		return ErrDuplicateJob
	}
	s.jobs[job.ID] = job.Clone()
	return nil
}

func (s *Storage) Update(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.jobs[job.ID]
	if !ok {
		return fmt.Errorf("job %d does not exist %w", job.ID, ErrJobNotFound)
	}
	s.jobs[job.ID] = job.Clone()
	return nil
}

func (s *Storage) Dump() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
		return job.Clone(), nil
	}
	return Job{}, ErrJobNotFound
}

func (s *Storage) ListIDs() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]int, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	return ids
}

func (s *Storage) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = make(map[int]Job)
}

func (s *Storage) Rename(id int, newName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("no such job by id: %w", ErrJobNotFound)
	}
	job.Name = newName
	s.jobs[id] = job
	return nil
}

func (s *Storage) RemoveByID(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
}

func (s *Storage) DumpTags() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if ok {
		job.Tags[tag] = struct{}{}
		s.jobs[id] = job
	}
}

func (s *Storage) Snapshot(w io.Writer) error {
	s.mu.RLock()
	version := 0
	ids := make([]int, 0, len(s.jobs))
	for k := range s.jobs {
		ids = append(ids, k)
	}
	slices.Sort(ids)
	var br strings.Builder
	br.WriteString("scheduler-snapshot v " + strconv.Itoa(version+1))
	br.WriteString("\n")
	for _, id := range ids {
		j := s.jobs[id]
		br.WriteString(strconv.Itoa(j.ID))
		br.WriteString(":")
		br.WriteString(strconv.Quote(j.Name))
		br.WriteString(":")
		br.WriteString(strconv.Itoa(int(j.State)))
		br.WriteString("\n")
	}
	data := br.String()
	bytes := []byte(data)
	s.mu.RUnlock()
	n, err := w.Write(bytes)
	if err != nil {
		return fmt.Errorf("error write snapshot: %w", err)
	}
	if n < len(bytes) {
		return fmt.Errorf("error write snapshot: short write: %w", io.ErrShortWrite)
	}
	return nil
}
