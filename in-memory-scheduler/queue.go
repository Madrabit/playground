package main

import (
	"errors"
	"sync"
)

type JobsQueue struct {
	ids        []int
	head, tail int
	size       int
	capacity   int
	/*
		нужно для сигналов cond
	*/
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
}

func NewJobsQueue() *JobsQueue {
	ids := make([]int, maxJobs)
	return &JobsQueue{ids: ids, capacity: maxJobs, cond: sync.NewCond(&sync.Mutex{})}
}

func (jb *JobsQueue) Len() int {
	return jb.size
}

func (jb *JobsQueue) Push(id int) error {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	if jb.closed {
		return errors.New("queue closed")
	}
	if jb.size == jb.capacity {
		return ErrQueueFull
	}
	jb.ids[jb.tail] = id
	jb.tail = (jb.tail + 1) % jb.capacity
	jb.size++
	jb.cond.Signal()
	return nil
}

func (jb *JobsQueue) Pop() int {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	for jb.size == 0 && !jb.closed {
		jb.cond.Wait()

	}
	if jb.size == 0 && jb.closed {
		return 0
	}
	id := jb.ids[jb.head]
	jb.head = (jb.head + 1) % jb.capacity
	jb.size--
	return id
}

func (jb *JobsQueue) Close() {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	jb.closed = true
	jb.cond.Broadcast()
}
