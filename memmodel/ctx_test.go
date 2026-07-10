package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func TestCtxPropagation(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelParent()
	child, cancelChild := context.WithTimeout(parent, 500*time.Millisecond)
	defer cancelChild()
	if errors.Is(child.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", child.Err())
	}
}

func TestCtxCanceled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("expected Canceled, got %v", ctx.Err())
	}
}

func TestCtxDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", ctx.Err())
	}
}

func worker(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func TestNoCancelGoroutineLeaks(t *testing.T) {
	fmt.Println("before: ", runtime.NumGoroutine())
	ctx, _ := context.WithCancel(context.Background())

	go worker(ctx)
	//cancel()
	time.Sleep(200 * time.Millisecond)
	fmt.Println("after: ", runtime.NumGoroutine())
}

func TestContextImmutability(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	child1, cancelChild1 := context.WithCancel(parent)
	child2, cancelChild2 := context.WithCancel(child1)
	defer cancelChild2()
	cancelChild1()
	if !errors.Is(child1.Err(), context.Canceled) {
		fmt.Println("expected canceled, got %w", child1.Err())
	}
	if !errors.Is(child2.Err(), context.Canceled) {
		fmt.Println("expected canceled, got %w", child2.Err())
	}
	if parent.Err() != nil {
		fmt.Println("expected nil, got %w", parent.Err())
	}
	cancelParent()
	if !errors.Is(child1.Err(), context.Canceled) {
		fmt.Println("expected canceled, got %w", child1.Err())
	}
	if !errors.Is(child2.Err(), context.Canceled) {
		fmt.Println("expected canceled, got %w", child2.Err())
	}
	if !errors.Is(parent.Err(), context.Canceled) {
		fmt.Println("expected canceled, got %w", parent.Err())
	}

}
