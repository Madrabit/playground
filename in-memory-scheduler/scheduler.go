package main

import (
	"fmt"
	"log"
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
	mu            sync.RWMutex
	workers       []*Worker
	stop          chan struct{}
	roundRobin    uint64
	results       chan Results
	processor     Processor
	wg            sync.WaitGroup
	goroutines    atomic.Int64
	validator     *Validator
	validatorOnce sync.Once
	activeWorkers map[int]time.Time
	heartBeat     chan HeartBeat
	cpus          int
	cond          *sync.Cond
	metrics       Metrics
	storage       *Storage // для имитации бага прямой доступ в структуру

}

type Results struct {
	JobID int
	State JobStatus
	Error error
}

func NewScheduler(processor Processor, cpus int) *Scheduler {
	queue := NewJobsQueue()
	storage := NewStorage()
	return &Scheduler{
		queue:         queue,
		admin:         storage,
		debug:         storage,
		reader:        storage,
		writer:        storage,
		stop:          make(chan struct{}),
		results:       make(chan Results, 1024),
		processor:     processor,
		activeWorkers: map[int]time.Time{},
		heartBeat:     make(chan HeartBeat, 1024),
		mu:            sync.RWMutex{},
		cpus:          cpus,
		cond:          sync.NewCond(&sync.Mutex{}),
		metrics:       Metrics{},
		validator:     NewValidator(),
		validatorOnce: sync.Once{},
	}
}

func (s *Scheduler) Start() {
	s.StartWorkers(s.cpus)
	s.StartValidator()
	go func() {
		for w := range s.heartBeat {
			s.mu.Lock()
			s.activeWorkers[w.jobId] = w.LastSeen
			s.mu.Unlock()
		}
	}()
}

func (s *Scheduler) Stop() {
	close(s.stop)
	for _, w := range s.workers {
		s.StopWorker(w)
	}
	s.StopValidator()
	s.wg.Wait()
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

func (s *Scheduler) StartWorkers(cpus int) {
	n := cpus * 4
	s.workers = make([]*Worker, s.cpus*4)
	processor := Use(
		LoggingMiddleware,
		RetryMiddleware(3),
	)(s.processor)

	for i := 0; i < n; i++ {
		w := NewWorker(i, s.results, processor, s.heartBeat)
		s.workers[i] = w
	}
	for _, w := range s.workers {
		s.StartWorker(w)
	}
	s.wg.Add(1)
	s.goroutines.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.goroutines.Add(-1)
		s.DispatchLoop()
	}()
	s.wg.Add(1)
	s.goroutines.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.goroutines.Add(-1)
		s.handleResults()
	}()
}

func (s *Scheduler) DispatchLoop() {
	for {
		select {
		case <-s.stop:
			return
		default:

		}
		id := s.queue.Pop()
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

func (s *Scheduler) StartWorker(w *Worker) {
	s.wg.Add(1)
	s.goroutines.Add(1)
	go func() {
		defer s.goroutines.Add(-1)
		defer s.wg.Done()
		w.Start()
	}()
}

func (s *Scheduler) StopWorker(w *Worker) {
	close(w.stop)
}

func (s *Scheduler) StartValidator() {
	go func() {
		s.validator.Loop()
	}()
	go func() {
		for e := range s.validator.ErrChan() {
			if e != nil {
				log.Println(e)
			}
		}
	}()
}

func (s *Scheduler) StopValidator() {
	s.validator.Stop()
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

func (s *Scheduler) ValidateAsync() {
	ids := s.reader.ListIDs()
	for _, id := range ids {
		job, err := s.reader.GetByID(id)
		if err != nil {
			continue
		}
		jobCopy := job
		go func(j Job) {
			s.validator.Validate(j)
		}(jobCopy)
	}
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

func (s *Scheduler) CloseQueue() {
	s.queue.Close()
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
