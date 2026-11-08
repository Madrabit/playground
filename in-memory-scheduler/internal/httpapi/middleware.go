package httpapi

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type HTTPMiddleware func(http.Handler) http.Handler

// Собирает middleware в одну цепочку.
// Первый middleware в списке выполняется первым.
func HTTPUse(handler http.Handler, middlewares ...HTTPMiddleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// -------------------- Request ID --------------------

type requestIDKey struct{}

var requestCounter atomic.Uint64

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strconv.FormatUint(requestCounter.Add(1), 10)

		w.Header().Set("X-Request-ID", id)

		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// -------------------- Access Log --------------------

// Запоминает HTTP-статус, который отправляет обработчик.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}

	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}

	return w.ResponseWriter.Write(data)
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w}

		defer func() {
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}

			log.Printf(
				"access request_id=%s method=%s path=%q status=%d",
				requestID(r.Context()),
				r.Method,
				r.URL.Path,
				status,
			)
		}()

		next.ServeHTTP(sw, r)
	})
}

// -------------------- Panic Recovery --------------------

func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf(
					"panic request_id=%s method=%s path=%q: %v",
					requestID(r.Context()),
					r.Method,
					r.URL.Path,
					recovered,
				)

				http.Error(
					w,
					http.StatusText(http.StatusInternalServerError),
					http.StatusInternalServerError,
				)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// -------------------- Request Duration --------------------

func RequestDuration(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		defer func() {
			log.Printf(
				"duration request_id=%s path=%q elapsed=%s",
				requestID(r.Context()),
				r.URL.Path,
				time.Since(start),
			)
		}()

		next.ServeHTTP(w, r)
	})
}

// -------------------- Concurrency Guard --------------------

func ConcurrencyLimit(max int) HTTPMiddleware {
	if max <= 0 {
		panic("ConcurrencyLimit: max must be positive")
	}

	slots := make(chan struct{}, max)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case slots <- struct{}{}:
				defer func() {
					<-slots
				}()

				next.ServeHTTP(w, r)

			default:
				http.Error(
					w,
					http.StatusText(http.StatusServiceUnavailable),
					http.StatusServiceUnavailable,
				)
			}
		})
	}
}
