package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Scheduler struct {
	queue      *JobsQueue
	admin      StoreAdmin
	debug      StoreDebug
	reader     StoreReader
	writer     StoreWriter
	mu         sync.Mutex
	workers    []*Worker
	stop       chan struct{}
	roundRobin uint64
	results    chan Results
	processor  Processor
}

type Results struct {
	JobID int
	State JobStatus
	Error error
}

func NewScheduler(processor Processor) *Scheduler {
	queue := NewJobsQueue()
	storage := NewStorage()
	return &Scheduler{
		queue:     queue,
		admin:     storage,
		debug:     storage,
		reader:    storage,
		writer:    storage,
		stop:      make(chan struct{}),
		results:   make(chan Results, 1024),
		processor: processor,
	}
}

func (s *Scheduler) Add(j Job) error {
	err := s.writer.Save(j)
	if err != nil {
		return fmt.Errorf("scheduler add job: %d %w", j.ID, err)
	}
	err = s.queue.Push(j.ID)
	if err != nil {
		return fmt.Errorf("scheduler add job: %d %w", j.ID, err)
	}
	return nil
}

func (s *Scheduler) AddAndLog(j Job) error {
	return s.Add(j)
}

func (s *Scheduler) StartWorkers(n int) {
	s.workers = make([]*Worker, n)
	processor := Use(
		LoggingMiddleware,
		RetryMiddleware(3),
	)(s.processor)

	for i := 0; i < n; i++ {
		w := NewWorker(i, s.results, processor)
		s.workers[i] = w
	}

	for _, w := range s.workers {
		w.Start()
	}

	/*
		кто запускает? шедулер
		кто останавливает? никто
		кто ждёт завершения? никто
		что будет при Stop()? перестанут генериться новые воркеры
	*/
	go s.DispatchLoop()
	/*
		   кто запускает? шедулер
			кто останавливает? никто
			кто ждёт завершения? никто
		   что будет при Stop()? перестанут обрабатываться результаты из воркеров
	*/
	go s.handleResults()
}

func (s *Scheduler) Stop() {
	close(s.stop)
	for _, w := range s.workers {
		w.Stop()
	}
}

func (s *Scheduler) DispatchLoop() {
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		id, ok := s.queue.Pop()
		if !ok {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		job, err := s.reader.GetByID(id)
		if err != nil {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		worker := s.pickWorker()
		job.State = Running
		worker.Enqueue(job)
	}
}

func (s *Scheduler) handleResults() {
	for {
		select {
		case <-s.stop:
			return
		case result := <-s.results:
			job, err := s.reader.GetByID(result.JobID)
			if err != nil {
				continue
			}
			if result.Error != nil {
				job.State = Failed
			} else {
				job.State = result.State
			}
			err = s.writer.Update(job)
			if err != nil {
				fmt.Println("failed to update")
				continue
			}
		}
	}
}

func (s *Scheduler) pickWorker() *Worker {
	i := atomic.AddUint64(&s.roundRobin, 1)
	return s.workers[i%uint64(len(s.workers))]
}

func (s *Scheduler) ValidateAsync() []error {
	errorsChan := make(chan error, 1024)
	ids := s.reader.ListIDs()
	var wg sync.WaitGroup
	wg.Add(len(ids))
	var errs []error
	for _, id := range ids {
		job, err := s.reader.GetByID(id)
		if err != nil {
			errorsChan <- fmt.Errorf("scheduler validation error: job id: %d, %w", id, ErrJobNotFound)
			wg.Done()
			continue
		}
		jobCopy := job

		go func(j Job) {
			defer wg.Done()
			err := ValidateJob(j)
			if err != nil {
				errorsChan <- fmt.Errorf("scheduler validation error: job id: %d, %w", j.ID, err)
			}
		}(jobCopy)
	}
	wg.Wait()
	close(errorsChan)
	for e := range errorsChan {
		errs = append(errs, e)
	}
	return errs
}

func (s *Scheduler) ProcessBatch(ids []int) []error {
	var errs []error
	for _, id := range ids {
		job, err := s.reader.GetByID(id)
		if err != nil {
			errs = append(errs, fmt.Errorf("scheduler process batch: job id: %d, %w", id, ErrJobNotFound))
			continue
		}
		if _, ok := job.Tags["fatal"]; ok {
			errs = append(errs, fmt.Errorf("scheduler process batch: job id: %d, %w", job.ID, ErrProcessAborted))
			break
		}
		if _, ok := job.Tags["skip"]; ok {
			continue
		}
	}
	return errs
}

func (s *Scheduler) NormalizeTags() error {
	jobs := s.reader.ListIDs()
	for _, id := range jobs {
		job, err := s.reader.GetByID(id)
		if err == nil {
			tags := job.Tags
			newTags := make(map[string]struct{})
			for tag := range tags {
				tag = normalize(tag)
				if tag != "" {
					newTags[tag] = struct{}{}
				}
			}
			job.Tags = newTags
			err := s.writer.Update(job)
			if err != nil {
				return fmt.Errorf("scheduler normalize tags: jobID %d %w", job.ID, err)
			}
		}
	}
	return nil
}

func normalize(tag string) string {
	return strings.ToLower(strings.TrimSpace(tag))
}

func (s *Scheduler) BulkComplexityIncrease() error {
	jobs := s.reader.ListIDs()
	for _, id := range jobs {
		job, err := s.reader.GetByID(id)
		if err == nil {
			job.Complexity = job.Complexity + 1
			err := s.writer.Update(job)
			if err != nil {
				return fmt.Errorf("scheduler bulk increase: jobID %d %w", id, err)
			}
		}
	}
	return nil
}

func (s *Scheduler) BulkRename() error {
	jobs := s.reader.ListIDs()
	for _, id := range jobs {
		job, err := s.reader.GetByID(id)
		if err == nil {
			newName := job.Name + ":" + strconv.Itoa(id)
			err := s.writer.Rename(job.ID, newName)
			if err != nil {
				return fmt.Errorf("scheduler bulk rename: jobID %d %w", id, err)
			}
		}
	}
	return nil
}

func (s *Scheduler) RemoveCancelled() {
	jobs := s.reader.ListIDs()
	for _, id := range jobs {
		job, err := s.reader.GetByID(id)
		if err == nil {
			if job.State == Cancelled {
				s.writer.RemoveByID(job.ID)
			}
		}
	}
}

func (s *Scheduler) CollectRunningJobs() []int {
	var ids []int
	jobs := s.reader.ListIDs()
	for _, id := range jobs {
		job, err := s.reader.GetByID(id)
		if err == nil {
			if job.State != Running {
				continue
			}
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *Scheduler) GetById(ID int) (Job, error) {
	job, err := s.reader.GetByID(ID)
	if err != nil {
		return Job{}, fmt.Errorf("scheduler GetById: jobID %d %w", ID, err)
	}
	return job, nil
}
