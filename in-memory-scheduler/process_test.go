package main

import "testing"

func Benchmark_Interface(b *testing.B) {
	var p Processor = &PrintProcessor{}

	job := Job{ID: 1}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.Process(job)
	}
}

func Benchmark_DirectCall(b *testing.B) {
	var p PrintProcessor = PrintProcessor{}

	job := Job{ID: 1}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.Process(job)
	}
}
