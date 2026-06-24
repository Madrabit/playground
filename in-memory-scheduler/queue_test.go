package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQueuePushPop(t *testing.T) {
	q := NewJobsQueue()
	if err := q.Push(1); err != nil {
		t.Errorf("unexpected error on push: %v", err)
	}
	id, ok := q.Pop()
	if !ok {
		t.Errorf("unexpected error on pop: %v", id)
	}
	if id != 1 {
		t.Errorf("unexpected id on pop: %v", id)
	}
}

func TestQueueFIFO(t *testing.T) {
	q := NewJobsQueue()
	for i := 0; i < 10; i++ {
		err := q.Push(i)
		if err != nil {
			t.Errorf("unexpected error on push: %v", err)
		}
	}
	for i := 0; i < 10; i++ {
		id, ok := q.Pop()
		if !ok {
			t.Errorf("unexpected error on pop: %v", id)
		}
		if id != i {
			t.Errorf("unexpected id on pop: %v", id)
		}
	}
}

func TestQueuePopBlockedUntilPush(t *testing.T) {
	q := NewJobsQueue()
	done := make(chan int)
	go func() {
		id, ok := q.Pop()
		if !ok {
			t.Errorf("unexpected error on pop: %v", id)
		}
		done <- id
	}()
	time.Sleep(50 * time.Millisecond) //даем время Pop дождаться Push
	q.Push(42)
	select {
	case id := <-done:
		if id != 42 {
			t.Errorf("unexpected id on pop: %v", id)
		}
	case <-time.After(200 * time.Millisecond):
		t.Errorf("Pop() did not unblock after Push()")
	}
}

func TestQueuePopUnblockAfterClose(t *testing.T) {
	q := NewJobsQueue()
	done := make(chan struct{})
	go func() {
		_, ok := q.Pop()
		if ok {
			fmt.Errorf("expected ok == false after close")
		}
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	q.Close() //close пошлет Broadcast и Pop должен вернуть 0, false
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Pop() did not unblock after Close")
	}
}

func TestQueueConcurrentPushPop(t *testing.T) {
	q := NewJobsQueue()
	wg := sync.WaitGroup{}
	const N = 1000
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			q.Push(i)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			q.Pop()
		}
	}()
	wg.Wait()

}
