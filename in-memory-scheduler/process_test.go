package main

import (
	"context"
	"testing"
)

func Benchmark_Interface(b *testing.B) {
	var p Processor = &PrintProcessor{}

	job := Job{ID: 1}

	b.ResetTimer()
	ctx, _ := context.WithCancel(context.Background())
	for i := 0; i < b.N; i++ {
		p.Process(ctx, job)
	}
}

func Benchmark_DirectCall(b *testing.B) {
	var p PrintProcessor = PrintProcessor{}

	job := Job{ID: 1}
	ctx, _ := context.WithCancel(context.Background())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Process(ctx, job)
	}
}
