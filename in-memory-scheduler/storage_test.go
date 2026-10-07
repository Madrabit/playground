package main

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestSnapshotWriter(t *testing.T) {
	storage := NewStorage()
	jobs := []Job{
		{ID: 3, Name: "Third job", State: Failed},
		{ID: 1, Name: "First job", State: Pending},
		{ID: 2, Name: "Second job", State: Running},
	}
	err := storage.Save(jobs[0])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	err = storage.Save(jobs[1])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	err = storage.Save(jobs[2])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := storage.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	expected := "scheduler-snapshot v 1\n" +
		"1:\"First job\":0\n" +
		"2:\"Second job\":1\n" +
		"3:\"Third job\":2\n"
	if expected != buf.String() {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, buf.String())
	}
}

func TestSnapshotWriterCutted(t *testing.T) {
	storage := NewStorage()
	jobs := []Job{
		{ID: 3, Name: "Third job", State: Failed},
		{ID: 1, Name: "First job", State: Pending},
		{ID: 2, Name: "Second job", State: Running},
	}
	err := storage.Save(jobs[0])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	err = storage.Save(jobs[1])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	err = storage.Save(jobs[2])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var wr shortWriter
	snapshotErr := storage.Snapshot(&wr)
	if !errors.Is(snapshotErr, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", snapshotErr)
	}
}

type shortWriter struct{}

func (s *shortWriter) Write(p []byte) (int, error) {
	return min(3, len(p)), nil
}

type failingWriter struct {
	buf bytes.Buffer
	err error
}

var testErr = errors.New("test write failure")

func (s *failingWriter) Write(p []byte) (int, error) {
	count := min(10, len(p))
	n, _ := s.buf.Write(p[:count])
	return n, s.err
}

func TestSnapshotWriterCorrupted(t *testing.T) {
	storage := NewStorage()
	jobs := []Job{
		{ID: 3, Name: "Third job", State: Failed},
		{ID: 1, Name: "First job", State: Pending},
		{ID: 2, Name: "Second job", State: Running},
	}
	err := storage.Save(jobs[0])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	err = storage.Save(jobs[1])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	err = storage.Save(jobs[2])
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	wr := failingWriter{err: testErr}
	snapshotErr := storage.Snapshot(&wr)
	if !errors.Is(snapshotErr, testErr) {
		t.Fatalf("expected  %v", snapshotErr)
	}
	if wr.buf.String() != "scheduler-" {
		t.Fatalf("expected prefix %q, got %q", "scheduler-", wr.buf.String())
	}
}
