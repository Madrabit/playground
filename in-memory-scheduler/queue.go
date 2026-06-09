package main

import "sync"

type JobsQueue struct {
	ids        []int
	head, tail int
	size       int
	capacity   int
	mu         sync.Mutex
}

func NewJobsQueue() *JobsQueue {
	ids := make([]int, maxJobs)
	return &JobsQueue{ids: ids, capacity: maxJobs}
}

func (jb *JobsQueue) Push(id int) error {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	if jb.size == jb.capacity {
		return ErrQueueFull
	}
	jb.ids[jb.tail] = id
	jb.tail = (jb.tail + 1) % jb.capacity
	jb.size++
	return nil
}

func (jb *JobsQueue) Pop() (int, bool) {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	if jb.size == 0 {
		return 0, false
	}
	id := jb.ids[jb.head]
	jb.head = (jb.head + 1) % jb.capacity
	jb.size--
	return id, true
}

func (jb *JobsQueue) Len() int {
	return jb.size
}
