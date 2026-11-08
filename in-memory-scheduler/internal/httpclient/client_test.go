package httpclient

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Проверяем, что клиент переиспользует одно соединение.
func TestClientConnectionReuse(t *testing.T) {
	var newConnections atomic.Int32

	server := httptest.NewUnstartedServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(
				`{"done":10,"failed":2,"cancelled":1}`,
			))
		}),
	)

	server.Config.ConnState = func(
		conn net.Conn,
		state http.ConnState,
	) {
		if state == http.StateNew {
			newConnections.Add(1)
		}
	}

	server.Start()
	defer server.Close()

	client, err := NewClient(server.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	// Выполняем 20 последовательных запросов
	// через один экземпляр HTTP-клиента.
	for i := 0; i < 20; i++ {
		stats, err := client.GetStats(context.Background())
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}

		if stats.Done != 10 {
			t.Fatalf("Done = %d, want 10", stats.Done)
		}
	}

	if got := newConnections.Load(); got != 1 {
		t.Errorf(
			"created %d TCP connections, want 1",
			got,
		)
	}
}

// Проверяем отмену HTTP-запроса через context.
func TestClientRequestCancellation(t *testing.T) {
	started := make(chan struct{})
	cancelObserved := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerDone)

			close(started)

			<-r.Context().Done()

			close(cancelObserved)

			// Не отправляем ответ до получения сигнала от теста.
			<-release
		},
	))
	defer server.Close()

	client, err := NewClient(server.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		_, err := client.GetStats(ctx)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}

	cancel()

	select {
	case <-cancelObserved:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not observe request cancellation")
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"error = %v, want context.Canceled",
				err,
			)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("client request did not finish")
	}

	close(release)

	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("server handler did not finish")
	}
}

// Проверяем общий timeout HTTP-клиента.
func TestClientTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			close(started)

			<-r.Context().Done()
			<-release
		},
	))
	defer server.Close()

	client, err := NewClient(server.URL, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	select {
	default:
	}

	_, err = client.GetStats(context.Background())

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf(
			"error = %v, want context.DeadlineExceeded",
			err,
		)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("server handler did not start")
	}

	close(release)
}

// Тестовый HTTP transport.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(
	r *http.Request,
) (*http.Response, error) {
	return f(r)
}

// Body, который запоминает факт закрытия.
type trackedBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *trackedBody) Close() error {
	b.closed.Store(true)
	return nil
}

// Проверяем, что клиент закрывает response body.
func TestClientClosesResponseBody(t *testing.T) {
	body := &trackedBody{
		Reader: strings.NewReader(
			`{"done":1,"failed":0,"cancelled":0}`,
		),
	}

	client := &Client{
		baseURL: "http://scheduler",
		httpClient: &http.Client{
			Transport: roundTripFunc(
				func(r *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       body,
						Request:    r,
					}, nil
				},
			),
		},
	}

	_, err := client.GetStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !body.closed.Load() {
		t.Error("response body was not closed")
	}
}
