package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"playground/in-memory-scheduler/internal/domain"
	"playground/in-memory-scheduler/internal/scheduler"
)

type cancelWaitMock struct {
	calls   atomic.Int32
	polling chan struct{}
}

func (m *cancelWaitMock) Add(domain.Job) error {
	return nil
}

func (m *cancelWaitMock) Stats() scheduler.SchedulerStats {
	return scheduler.SchedulerStats{}
}

func (m *cancelWaitMock) GetById(id int) (domain.Job, error) {
	if m.calls.Add(1) == 2 {
		close(m.polling)
	}

	// Состояние задачи не меняется.
	return domain.Job{ID: id}, nil
}

func TestWaitJobClientCancellation(t *testing.T) {
	mock := &cancelWaitMock{
		polling: make(chan struct{}),
	}

	jobHandler := NewJobHandler(mock)

	// Сигнал о завершении серверного обработчика.
	handlerExited := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /jobs/{id}/wait",
		func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerExited)
			jobHandler.handleWaitJob(w, r)
		},
	)

	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		server.URL+"/jobs/42/wait",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		status int
		err    error
	}

	clientDone := make(chan result, 1)

	go func() {
		resp, err := server.Client().Do(req)
		if err != nil {
			clientDone <- result{err: err}
			return
		}

		defer resp.Body.Close()
		clientDone <- result{status: resp.StatusCode}
	}()

	// Дожидаемся реального начала ожидания на сервере.
	select {
	case <-mock.polling:
		// GetById вызван минимум дважды.

	case res := <-clientDone:
		t.Fatalf(
			"request finished before cancellation: status=%d err=%v",
			res.status,
			res.err,
		)

	case <-time.After(3 * time.Second):
		t.Fatalf(
			"server did not start polling; GetById calls=%d",
			mock.calls.Load(),
		)
	}

	// Клиент отменяет HTTP-запрос.
	cancel()

	// Серверный обработчик обязан завершиться.
	select {
	case <-handlerExited:
		// Успех.

	case <-time.After(3 * time.Second):
		t.Fatal("server handler did not stop after cancellation")
	}

	// Клиент должен получить ошибку отмены.
	select {
	case res := <-clientDone:
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf(
				"client error=%v (status=%d), want context.Canceled",
				res.err,
				res.status,
			)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("client request did not finish")
	}
}
