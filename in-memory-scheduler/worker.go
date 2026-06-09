package main

type Worker struct {
	ID        int
	stop      chan struct{}
	jobs      chan Job
	results   chan<- Results
	processor Processor
}

type Processor interface {
	Process(job Job) (JobStatus, error)
}

func NewWorker(ID int, results chan<- Results, processor Processor) *Worker {
	return &Worker{
		ID:        ID,
		stop:      make(chan struct{}),
		jobs:      make(chan Job),
		results:   results,
		processor: processor,
	}
}

func (w *Worker) Start() {
	/*
		кто запускает? в шедулере цикл с воркерами
		кто останавливает? никто
		кто ждёт завершения? никто не ждет
		что будет при Stop()? цикл Loop у однго воркера остановится
	*/
	go w.Loop()
}

func (w *Worker) Stop() {
	close(w.stop)
}

func (w *Worker) Loop() {
	for {
		select {
		case <-w.stop:
			return
		case job := <-w.jobs:
			state, err := w.Process(job)
			w.results <- Results{
				job.ID,
				state,
				err,
			}
		}
	}
}

//go:noinline
func (w *Worker) Process(j Job) (JobStatus, error) {
	process, err := w.processor.Process(j)
	return process, err
}

func (w *Worker) Enqueue(job Job) {
	w.jobs <- job
}
