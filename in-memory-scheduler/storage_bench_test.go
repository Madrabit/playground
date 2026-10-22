package main

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"
)

func BenchmarkStorage_Snapshot(b *testing.B) {
	b.ReportAllocs()

	storage := NewStorage()
	var buf bytes.Buffer
	for i := range 1000 {
		err := storage.Save(Job{ID: i, Name: "name" + strconv.Itoa(i)})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		err := storage.Snapshot(&buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_SnapshotFlush(b *testing.B) {
	b.ReportAllocs()

	storage := NewStorage()
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, 16384)
	for i := range 1000 {
		err := storage.Save(Job{ID: i, Name: "name" + strconv.Itoa(i)})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		err := storage.Snapshot(writer)
		if err != nil {
			b.Fatal(err)
		}
		err = writer.Flush()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_Restore(b *testing.B) {
	b.ReportAllocs()
	storage := NewStorage()
	var buf bytes.Buffer
	for i := range 1000 {
		err := storage.Save(Job{ID: i, Name: "name" + strconv.Itoa(i)})
		if err != nil {
			b.Fatal(err)
		}
	}
	err := storage.Snapshot(&buf)
	if err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()
	reader := bytes.NewReader(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(data)
		err := storage.Restore(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_RestoreSized(b *testing.B) {
	b.ReportAllocs()
	storage := NewStorage()
	var buf bytes.Buffer
	for i := range 1000 {
		err := storage.Save(Job{ID: i, Name: "name" + strconv.Itoa(i)})
		if err != nil {
			b.Fatal(err)
		}
	}
	err := storage.Snapshot(&buf)
	if err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()
	reader := bytes.NewReader(data)
	buffered := bufio.NewReaderSize(reader, 16384)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(data)
		buffered.Reset(reader)
		err := storage.Restore(buffered)
		if err != nil {
			b.Fatal(err)
		}
	}
}
