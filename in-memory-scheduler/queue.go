package main

import (
	"errors"
	"fmt"
	"runtime"
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
	q := &JobsQueue{
		ids:      ids,
		capacity: maxJobs,
		mu:       sync.Mutex{}}
	q.cond = sync.NewCond(&q.mu)
	return q
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
	fmt.Println("Push called id=", id, "size now=", jb.size)

	return nil
}

func (jb *JobsQueue) Pop() (int, bool) {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	fmt.Println("Pop called, size=", jb.size, "closed=", jb.closed)
	for jb.size == 0 && !jb.closed {
		jb.cond.Wait()

	}
	if jb.size == 0 && jb.closed {
		return 0, false
	}
	id := jb.ids[jb.head]
	jb.head = (jb.head + 1) % jb.capacity
	jb.size--
	return id, true
}

func (jb *JobsQueue) Close() {
	fmt.Println("Close called")
	jb.mu.Lock()
	defer jb.mu.Unlock()
	jb.closed = true
	jb.cond.Broadcast()
	// debug
	buf := make([]byte, 1<<10)
	n := runtime.Stack(buf, false)
	fmt.Println("JobsQueue.Close called, stack:\n", string(buf[:n]))
}
