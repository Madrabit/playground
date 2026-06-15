package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	done      atomic.Uint64
	failed    atomic.Uint64
	cancelled atomic.Uint64
}

type Scheduler struct {
	queue  *JobsQueue
	admin  StoreAdmin
	debug  StoreDebug
	reader StoreReader
	writer StoreWriter
	/*
		блокировка для сбора в мапу с разных воркеров activeWorkers
		а также MoveJob и CloneJob для демонстрационного кода
	*/
	mu                sync.RWMutex
	workers           []*Worker
	stop              chan struct{}
	roundRobin        uint64
	results           chan Results
	wg                sync.WaitGroup
	validator         *Validator
	validatorOnce     sync.Once
	activeWorkers     map[int]time.Time
	heartBeat         chan HeartBeat
	cond              *sync.Cond
	metrics           Metrics
	storage           *Storage // для имитации бага прямой доступ в структуру
	validationRequest chan ValidationRequest
	validationResult  chan ValidationResult
}

type Results struct {
	JobID int
	State JobStatus
	Error error
}

func NewScheduler(
	validator *Validator,
	queue *JobsQueue,
	validationRequest chan ValidationRequest,
	result chan Results,
	heartBeat chan HeartBeat,
) *Scheduler {
	storage := NewStorage()
	s := &Scheduler{
		queue:             queue,
		admin:             storage,
		debug:             storage,
		reader:            storage,
		writer:            storage,
		stop:              make(chan struct{}),
		results:           result,
		activeWorkers:     map[int]time.Time{},
		heartBeat:         heartBeat,
		mu:                sync.RWMutex{},
		metrics:           Metrics{},
		validator:         validator,
		validatorOnce:     sync.Once{},
		validationRequest: validationRequest,
		validationResult:  make(chan ValidationResult, 1024),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Scheduler) Start() {
	go func() {
		for w := range s.heartBeat {
			s.mu.Lock()
			s.activeWorkers[w.jobId] = w.LastSeen
			s.mu.Unlock()
		}
	}()
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

type ValidationRequest struct {
	JobID int
	job   Job
	reply chan ValidationResult
}

type ValidationResult struct {
	JobID int
	job   Job
	Err   error
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
			continue
		}
		job, err := s.reader.GetByID(id)
		if err != nil {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		replyChan := make(chan ValidationResult, 1)
		request := ValidationRequest{
			JobID: id,
			job:   job,
			reply: replyChan,
		}
		s.validationRequest <- request
		result := <-replyChan
		if result.Err != nil {
			// пока так
			fmt.Printf("scheduler handle validation result job: %d %w", result.JobID)
		}
		worker := s.pickWorker()
		worker.Enqueue(result.job)
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
				s.metrics.failed.Add(1)
			} else {
				job.State = result.State
				s.metrics.done.Add(1)
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

func (s *Scheduler) ProcessBatch(ids []int) []error {
	var wg sync.WaitGroup
	var errChan = make(chan error, 1024)
	var errs []error
	for _, jobId := range ids {
		id := jobId
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := s.reader.GetByID(id)
			if err != nil {
				errChan <- fmt.Errorf("scheduler process batch: job id: %d, %w", id, ErrJobNotFound)
				return
			}
			if _, ok := job.Tags["fatal"]; ok {
				errChan <- fmt.Errorf("scheduler process batch: job id: %d, %w", job.ID, ErrProcessAborted)
				return
			}
			if _, ok := job.Tags["skip"]; ok {
				return
			}
		}()
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		errs = append(errs, err)
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

func (s *Scheduler) ActiveWorkers() []int {
	s.mu.RLock()
	defer s.mu.Unlock()
	active := make([]int, 0)
	for k, _ := range s.activeWorkers {
		active = append(active, k)
	}
	return active
}

type SchedulerStats struct {
	Done      uint
	Failed    uint
	Cancelled uint
}

func (s *Scheduler) Stats() SchedulerStats {
	return SchedulerStats{
		Done:      uint(s.metrics.done.Load()),
		Failed:    uint(s.metrics.failed.Load()),
		Cancelled: uint(s.metrics.cancelled.Load()),
	}
}

func (s *Scheduler) MoveJob() {
	s.mu.Lock()
	s.storage.mu.Lock()
	fmt.Println("move job")
	s.storage.mu.Unlock()
	s.mu.Unlock()
}

func (s *Scheduler) CloneJob() {
	s.mu.Lock()
	s.storage.mu.Lock()
	fmt.Println("move job")
	s.storage.mu.Unlock()
	s.mu.Unlock()
}
