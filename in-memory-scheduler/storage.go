package main

import (
	"bufio"
	"errors"
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
	type snapShot struct {
		ID    int
		Name  string
		State int
	}
	s.mu.RLock()

	ids := make([]int, 0, len(s.jobs))
	for k := range s.jobs {
		ids = append(ids, k)
	}
	slices.Sort(ids)
	jobs := make([]snapShot, 0, len(ids))
	for _, id := range ids {
		j := s.jobs[id]
		jobs = append(jobs, snapShot{
			ID:    id,
			Name:  j.Name,
			State: int(j.State),
		})
	}
	s.mu.RUnlock()
	version := 0
	if _, err := fmt.Fprintln(w, "scheduler-snapshot v "+strconv.Itoa(version+1)); err != nil {
		return fmt.Errorf("scheduler-snapshot v: %w", err)
	}
	for _, j := range jobs {
		_, err := fmt.Fprintf(
			w, "%d:%s:%d\n",
			j.ID,
			strconv.Quote(j.Name),
			j.State,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

type storageReader struct {
	data []byte
	pos  int64
}

func (r *storageReader) Read(p []byte) (int, error) {
	if r.pos >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += int64(n)
	return n, nil
}

func (s *Storage) Restore(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	jobs := make(map[int]Job)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("restore header: %w", err)
		}
		return fmt.Errorf("restore: missing header")
	}
	header := scanner.Text()
	if header != "scheduler-snapshot v 1" {
		return fmt.Errorf("restore: unsupported header %q", header)
	}
	for scanner.Scan() {
		line := scanner.Text()
		firstIndex := strings.Index(line, ":")
		if firstIndex == -1 {
			return fmt.Errorf("invalid line: %s", line)
		}
		secondIndex := strings.LastIndex(line, ":")
		if secondIndex <= firstIndex {
			return fmt.Errorf("invalid line: %s", line)
		}
		id, err := strconv.Atoi(line[:firstIndex])
		if err != nil {
			return fmt.Errorf("error while parsing id: %w", err)
		}
		if _, ok := jobs[id]; ok {
			return fmt.Errorf("job %d already exists", id)
		}
		name, err := strconv.Unquote(line[firstIndex+1 : secondIndex])
		if err != nil {
			return fmt.Errorf("error while parsing name: %w", err)
		}
		state, err := strconv.Atoi(line[secondIndex+1:])
		if err != nil {
			return fmt.Errorf("error while parsing state: %w", err)
		}
		if state < int(Pending) || state > int(Stopped) {
			return fmt.Errorf("invalid state: %d", state)
		}
		job := Job{
			ID:    id,
			Name:  name,
			State: JobStatus(state),
		}
		jobs[job.ID] = job
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("restore scanner error %w", err)
	}
	s.mu.Lock()
	s.jobs = jobs
	s.mu.Unlock()
	return nil
}

type Sink interface {
	Write() io.Writer // это возвращаемое значение или тип?
}

func (s *Storage) Flush(sink Sink) error {
	err := s.Snapshot(sink.Write())
	if err != nil {
		return fmt.Errorf("error flushing snapshot %w", err)
	}
	return nil
}
func (s *Storage) TransferStorage(sink Sink) error {
	return transferSnapshot(s.Snapshot, sink)
}

func transferSnapshot(
	snapshot func(io.Writer) error,
	sink Sink,
) error {
	reader, writer := io.Pipe()
	exportDone := make(chan error, 1)

	go func() {
		err := snapshot(writer)
		_ = writer.CloseWithError(err)
		exportDone <- err
	}()
	_, copyErr := io.Copy(sink.Write(), reader)
	_ = reader.Close()
	exportErr := <-exportDone
	// Если ошибка пришла от Snapshot, сообщаем об экспорте.
	if copyErr != nil && exportErr != nil &&
		errors.Is(copyErr, exportErr) {
		return fmt.Errorf("export snapshot: %w", exportErr)
	}
	// Например, Sink.Write() вернул ошибку записи.
	if copyErr != nil {
		return fmt.Errorf("write snapshot to sink: %w", copyErr)
	}

	if exportErr != nil {
		return fmt.Errorf("export snapshot: %w", exportErr)
	}
	return nil
}
