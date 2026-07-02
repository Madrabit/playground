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
	normalQueue  Queue
	highQueue    Queue
	queueCounter int
	admin        StoreAdmin
	debug        StoreDebug
	reader       StoreReader
	writer       StoreWriter
	/*
		блокировка для сбора в мапу с разных воркеров activeWorkers
		а также MoveJob и CloneJob для демонстрационного кода
	*/
	mu                sync.RWMutex
	workers           []WorkerI
	stop              chan struct{}
	roundRobin        uint64
	results           []chan Results
	wg                sync.WaitGroup
	validator         *Validator
	validatorOnce     sync.Once
	activeWorkers     map[int]time.Time
	heartBeat         chan HeartBeat
	cond              *sync.Cond
	metrics           Metrics
	storage           *Storage // для имитации бага прямой доступ в структуру
	validationRequest chan ValidationRequest
	mergedChan        chan Results
	goroutines        atomic.Int64
	mergeWg           sync.WaitGroup
	jobsWg            sync.WaitGroup
}

type Results struct {
	JobID int
	State JobStatus
	Error error
}

func NewScheduler(
	validator *Validator,
	normalQueue *JobsQueue,
	highQueue *JobsQueue,
	validationRequest chan ValidationRequest,
	results []chan Results,
	heartBeat chan HeartBeat,
) *Scheduler {
	storage := NewStorage()
	s := &Scheduler{
		normalQueue:       normalQueue,
		highQueue:         highQueue,
		admin:             storage,
		debug:             storage,
		reader:            storage,
		writer:            storage,
		stop:              make(chan struct{}),
		results:           results,
		activeWorkers:     map[int]time.Time{},
		heartBeat:         heartBeat,
		mu:                sync.RWMutex{},
		metrics:           Metrics{},
		validator:         validator,
		validatorOnce:     sync.Once{},
		validationRequest: validationRequest,
		mergedChan:        make(chan Results, 1024),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

/*
Гарантии:
- activeWorkers получается LastSeen без гонок
- проверка мертвых воркеров
- все фоновые процессы Шедулера и работа их вплоть до Stop
*/
func (s *Scheduler) Start() {
	s.goSafe("DispatchLoop", s.DispatchLoop)
	s.goSafe("MergeResults", s.MergeResults)
	s.goSafe("HandleMergedResults", s.HandleMergedResults)
	s.goSafe("HeartBeatLoop", func() {
		for {
			select {
			case <-s.stop:
				return
			case w := <-s.heartBeat:
				s.mu.Lock()
				s.activeWorkers[w.jobId] = w.LastSeen
				s.mu.Unlock()
			}
		}
	})
	s.goSafe("HealthChecker", func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				s.mu.Lock()
				for jobId, lastSeen := range s.activeWorkers {
					if time.Since(lastSeen) > 10*time.Second {
						fmt.Printf("worker considered dead %d", jobId)
						delete(s.activeWorkers, jobId)
					}
				}
				s.mu.Unlock()
			}
		}
	})
}

func (s *Scheduler) goSafe(name string, fun func()) {
	s.wg.Add(1)
	s.goroutines.Add(1)
	go func() {
		defer fmt.Println("EXIT:", name)
		defer s.wg.Done()
		defer s.goroutines.Add(-1)
		fun()
	}()
}

/*
	Гарантии:

Закрытие всех очередей
Сигнал стоп всем зависимым командам
Все фоновые горутины доработают до конца
*/
func (s *Scheduler) Stop() {
	s.highQueue.Close()
	s.normalQueue.Close()
	close(s.stop)
	s.wg.Wait()
}

func (s *Scheduler) Add(j Job) error {
	const maxPendingJobs = 100
	var q Queue
	if s.queueCounter < 5 {
		q = s.highQueue
	} else {
		q = s.normalQueue
	}
	if q.Len() >= maxPendingJobs {
		return ErrQueueFull
	}
	err := q.Push(j.ID)
	if err != nil {
		return fmt.Errorf("scheduler add job: %d %w", j.ID, err)
	}
	err = s.writer.Save(j)
	if err != nil {
		return fmt.Errorf("scheduler add job: %d %w", j.ID, err)
	}
	s.queueCounter++
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
		fmt.Println("stop dispatcher before")
		select {
		case <-s.stop:
			fmt.Println("stop dispatcher")
			return
		default:
		}
		fmt.Println("stop dispatcher after")
		var id int
		var ok bool
		if s.queueCounter < 5 {
			id, ok = s.highQueue.Pop()
		} else {
			id, ok = s.normalQueue.Pop()
		}
		s.queueCounter--
		if !ok {
			return
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
			fmt.Errorf("scheduler handle validation result job: %d %w", result.JobID, result.Err)
		}
		worker := s.pickWorker()
		s.jobsWg.Add(1)
		worker.Enqueue(result.job)
	}
}

func (s *Scheduler) MergeResults() {

	for _, ch := range s.results {
		s.mergeWg.Add(1)
		go func(ch chan Results) {
			defer s.mergeWg.Done()
			for result := range ch {
				s.mergedChan <- result
			}
		}(ch)
	}
}

func (s *Scheduler) HandleMergedResults() {
	for {
		select {
		case <-s.stop:
			return
		case result, ok := <-s.mergedChan:
			if !ok {
				return
			}
			s.handleResults(result)
		}
	}
}

func (s *Scheduler) StopDispatch() {
	// Закрываем очереди — Pop() вернёт ok=false
	s.highQueue.Close()
	s.normalQueue.Close()

	// Пробуждаем всех, кто ждёт cond
	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()
}

func (s *Scheduler) handleResults(result Results) {
	job, err := s.reader.GetByID(result.JobID)
	if err != nil {
		return
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
		return
	}
}

func (s *Scheduler) pickWorker() WorkerI {
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
