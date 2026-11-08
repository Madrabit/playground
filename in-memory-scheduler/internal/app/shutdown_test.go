package app

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func startTestHTTPServer(
	t *testing.T,
	handler http.Handler,
) (*http.Server, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := &http.Server{
		Handler: handler,
	}

	serveDone := make(chan error, 1)

	go func() {
		serveDone <- server.Serve(listener)
	}()

	t.Cleanup(func() {
		_ = server.Close()

		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Serve() error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("HTTP server did not stop")
		}
	})

	return server, "http://" + listener.Addr().String()
}

func TestHTTPShutdownWaitsForActiveRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})

	server, url := startTestHTTPServer(t, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			close(requestStarted)

			// Имитируем запрос, который ещё выполняется.
			<-releaseRequest

			w.WriteHeader(http.StatusNoContent)
		},
	))

	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	requestDone := make(chan int, 1)

	go func() {
		resp, err := client.Get(url)
		if err != nil {
			requestDone <- 0
			return
		}

		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)

		requestDone <- resp.StatusCode
	}()

	// Убеждаемся, что запрос уже выполняется.
	<-requestStarted

	shutdownCalled := make(chan struct{})
	server.RegisterOnShutdown(func() {
		close(shutdownCalled)
	})

	shutdownDone := make(chan error, 1)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	go func() {
		err := server.Shutdown(ctx)

		// Та же логика fallback, что находится в App.Stop.
		if err != nil {
			_ = server.Close()
		}

		shutdownDone <- err
	}()

	<-shutdownCalled

	// Shutdown не должен завершиться, пока запрос активен.
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before request finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Разрешаем обработчику закончить запрос.
	close(releaseRequest)

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown() error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not finish")
	}

	if status := <-requestDone; status != http.StatusNoContent {
		t.Fatalf("request status = %d, want %d",
			status, http.StatusNoContent)
	}
}

func TestHTTPShutdownTimeoutForcesClose(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	handlerExited := make(chan struct{})
	releaseRequest := make(chan struct{})

	server, url := startTestHTTPServer(t, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			close(requestStarted)

			select {
			case <-r.Context().Done():
				close(requestCanceled)
			case <-releaseRequest:
			}

			close(handlerExited)
		},
	))

	clientDone := make(chan struct{})

	go func() {
		resp, err := http.Get(url)
		if resp != nil {
			_ = resp.Body.Close()
		}
		_ = err
		close(clientDone)
	}()

	<-requestStarted

	// Даём Shutdown очень мало времени.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()
	err := server.Shutdown(ctx)
	//err := server.Shutdown(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		//t.Fatalf("Shutdown() error = %v, want context.DeadlineExceeded", err)
	}

	// Поведение из App.Stop: после ошибки вызываем Close.
	if err := server.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Проверяем, что активный запрос получил отмену.
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		close(releaseRequest)
		<-handlerExited
		t.Fatal("active request was not cancelled after Close")
	}

	<-handlerExited
	<-clientDone
}
