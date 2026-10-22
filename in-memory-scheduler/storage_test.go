package main

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestRestoreSuccess(t *testing.T) {
	storage := NewStorage()
	jobs := []Job{
		{ID: 1, Name: "First job", State: Pending},
		{ID: 2, Name: "Second job", State: Running},
		{ID: 3, Name: "Third job", State: Failed},
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
	secondStorage := NewStorage()
	err = secondStorage.Save(Job{ID: 99, Name: "99 job", State: Failed})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	err = secondStorage.Restore(&buf)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	for _, expected := range jobs {
		job := secondStorage.jobs[expected.ID]
		if job.ID != expected.ID {
			t.Fatalf("expected job ID %d, got %d", jobs[expected.ID].ID, job.ID)
		}
		if job.Name != expected.Name {
			t.Fatalf("expected job Name %s, got %s", jobs[expected.ID].Name, job.Name)
		}
		if job.State != expected.State {
			t.Fatalf("expected job State %d, got %d", jobs[expected.ID].State, job.State)
		}
	}
	if _, ok := secondStorage.jobs[99]; ok {
		t.Fatalf("not expected job")
	}
	if len(secondStorage.jobs) != len(jobs) {
		t.Fatalf("expected %d jobs, got %d", len(jobs), len(secondStorage.jobs))
	}
}

func TestRestoreCutted(t *testing.T) {
	storage := NewStorage()
	text1 := "scheduler-snapshot v 1\n" +
		"99:\"99 job\":0\n"
	err := storage.Restore(strings.NewReader(text1))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	text2 := "scheduler-snapshot v 1\n" +
		"1:\"First job\":0\n" +
		"2:\"Second"
	err = storage.Restore(strings.NewReader(text2))
	if err == nil {
		t.Fatalf("did not catch cutted snapshote while restore: %v", err)
	}
	if len(storage.jobs) != 1 {
		t.Fatalf("expected %d jobs, got %d", 1, len(storage.jobs))
	}
	if storage.jobs[99].ID != 99 {
		t.Fatalf("expected job ID %d, got %d", 99, storage.jobs[99].ID)
	}
	if storage.jobs[99].Name != "99 job" {
		t.Fatalf("expected job Name %s, got %s", "99 job", storage.jobs[99].Name)
	}
	if storage.jobs[99].State != Pending {
		t.Fatalf("expected job State %d, got %d", Pending, storage.jobs[99].State)
	}
}

type chunkReader struct {
	data []byte
	pos  int64
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p[:min(3, len(p))], r.data[r.pos:])
	r.pos += int64(n)
	if r.pos >= int64(len(r.data)) {
		return n, io.EOF
	}
	return n, nil
}

func TestRestoreChunk(t *testing.T) {
	storage := NewStorage()
	text := "scheduler-snapshot v 1\n" +
		"99:\"99 job\":0\n"
	reader := &chunkReader{data: []byte(text)}
	err := storage.Restore(reader)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(storage.jobs) != 1 {
		t.Fatalf("expected %d jobs, got %d", 1, len(storage.jobs))
	}
	if storage.jobs[99].ID != 99 {
		t.Fatalf("expected job ID %d, got %d", 99, storage.jobs[99].ID)
	}
	if storage.jobs[99].Name != "99 job" {
		t.Fatalf("expected job Name %s, got %s", "99 job", storage.jobs[99].Name)
	}
	if storage.jobs[99].State != Pending {
		t.Fatalf("expected job State %d, got %d", Pending, storage.jobs[99].State)
	}
}

const testTimeout = 2 * time.Second

type testSink struct {
	writer io.Writer
}

func (s testSink) Write() io.Writer {
	return s.writer
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatal("timeout waiting for signal")
	}
}

func waitResult(t *testing.T, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(testTimeout):
		t.Fatal("transfer timed out; possible deadlock")
		return nil
	}
}

func notFinishedWithin(ch <-chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return false
	case <-time.After(d):
		return true
	}
}

// 1. Обычная передача данных.
func TestTransferSnapshot_Success(t *testing.T) {
	const want = "snapshot data"

	var dst bytes.Buffer

	snapshot := func(w io.Writer) error {
		_, err := io.WriteString(w, want)
		return err
	}

	err := transferSnapshot(snapshot, testSink{writer: &dst})
	if err != nil {
		t.Fatalf("transferSnapshot() error = %v", err)
	}

	if got := dst.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// 2. Получатель медленно принимает данные.
// Пока его Write заблокирован, экспорт не должен завершаться.
type slowWriter struct {
	dst     *bytes.Buffer
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *slowWriter) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.started)
	})

	<-w.release
	return w.dst.Write(p)
}

func TestTransferSnapshot_SlowSink(t *testing.T) {
	var dst bytes.Buffer

	sw := &slowWriter{
		dst:     &dst,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(sw.release) })
	}
	defer release()

	exportFinished := make(chan struct{})
	done := make(chan error, 1)

	snapshot := func(w io.Writer) error {
		defer close(exportFinished)

		_, err := io.WriteString(w, strings.Repeat("x", 1<<20))
		return err
	}

	go func() {
		done <- transferSnapshot(snapshot, testSink{writer: sw})
	}()

	waitSignal(t, sw.started)

	// Проверяем, что экспорт не завершился,
	// пока конечный Writer удерживает запись.
	blocked := notFinishedWithin(exportFinished, 50*time.Millisecond)

	release()

	if !blocked {
		t.Fatal("export finished while sink was blocked")
	}

	if err := waitResult(t, done); err != nil {
		t.Fatalf("transferSnapshot() error = %v", err)
	}

	if dst.Len() != 1<<20 {
		t.Fatalf("received %d bytes, want %d", dst.Len(), 1<<20)
	}
}

// 3. Экспортёр делает паузу между двумя частями.
// Получатель должен принять обе части.
func TestTransferSnapshot_SlowSnapshot(t *testing.T) {
	firstPartWritten := make(chan struct{})
	releaseWriter := make(chan struct{})
	transferFinished := make(chan struct{})
	done := make(chan error, 1)

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseWriter) })
	}
	defer release()

	var dst bytes.Buffer

	snapshot := func(w io.Writer) error {
		if _, err := io.WriteString(w, "first"); err != nil {
			return err
		}

		close(firstPartWritten)
		<-releaseWriter

		_, err := io.WriteString(w, "second")
		return err
	}

	go func() {
		err := transferSnapshot(snapshot, testSink{writer: &dst})
		done <- err
		close(transferFinished)
	}()

	waitSignal(t, firstPartWritten)

	// Перед второй частью перенос ещё не должен завершиться.
	stillWaiting := notFinishedWithin(transferFinished, 50*time.Millisecond)

	release()

	if !stillWaiting {
		t.Fatal("transfer finished before the second part was written")
	}

	if err := waitResult(t, done); err != nil {
		t.Fatalf("transferSnapshot() error = %v", err)
	}

	if got, want := dst.String(), "firstsecond"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// 4. Snapshot записал часть данных и вернул ошибку.
func TestTransferSnapshot_ExportFailsMidStream(t *testing.T) {
	errExport := errors.New("export failed")
	var dst bytes.Buffer

	snapshot := func(w io.Writer) error {
		if _, err := io.WriteString(w, "partial data"); err != nil {
			return err
		}
		return errExport
	}

	done := make(chan error, 1)
	go func() {
		done <- transferSnapshot(snapshot, testSink{writer: &dst})
	}()

	err := waitResult(t, done)

	if !errors.Is(err, errExport) {
		t.Fatalf("error = %v, want %v", err, errExport)
	}

	if got, want := dst.String(), "partial data"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
