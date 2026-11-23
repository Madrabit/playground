// ИЗМЕНЕНО В ЗАДАЧЕ 28.1: запросы и ответы клиента сверяются с общими HTTP DTO.
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"playground/in-memory-scheduler/internal/httpdto"
)

func equalJSONFields(a, b map[string]json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for key, want := range b {
		got, ok := a[key]
		if !ok || !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(want)) {
			return false
		}
	}
	return true
}

func TestCreateJobRequestContract(t *testing.T) {
	zero := 0
	tests := []struct {
		name       string
		complexity *int
		wantBody   string
	}{
		{
			name:       "missing complexity",
			complexity: nil,
			wantBody:   `{"name":"job","description":"details","attrs":{"env":"test"},"tags":{"batch":{}}}`,
		},
		{
			name:       "explicit zero complexity",
			complexity: &zero,
			wantBody:   `{"name":"job","description":"details","complexity":0,"attrs":{"env":"test"},"tags":{"batch":{}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				baseURL: "http://scheduler",
				httpClient: &http.Client{
					Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
						if r.Method != http.MethodPost || r.URL.Path != "/jobs" {
							t.Errorf("request = %s %s, want POST /jobs", r.Method, r.URL.Path)
						}
						if got := r.Header.Get("Content-Type"); got != "application/json" {
							t.Errorf("Content-Type = %q, want application/json", got)
						}
						defer r.Body.Close()
						var got, want map[string]json.RawMessage
						if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
							t.Fatalf("decode request: %v", err)
						}
						if err := json.Unmarshal([]byte(tt.wantBody), &want); err != nil {
							t.Fatalf("decode expected request: %v", err)
						}
						if !equalJSONFields(got, want) {
							t.Errorf("request body = %#v, want %#v", got, want)
						}
						return &http.Response{
							StatusCode: http.StatusCreated,
							Body:       io.NopCloser(strings.NewReader(`{"name":"job"}`)),
							Header:     make(http.Header),
							Request:    r,
						}, nil
					}),
				},
			}

			got, err := client.CreateJob(context.Background(), CreateJobRequest{
				Name:        "job",
				Description: "details",
				Complexity:  tt.complexity,
				Attrs:       map[string]string{"env": "test"},
				Tags:        map[string]struct{}{"batch": {}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != "job" {
				t.Errorf("response name = %q, want job", got.Name)
			}
		})
	}
}

func TestExportJobsStreamsAndStopsOnContextCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, `{"id":1,"name":"first","description":"","complexity":0,"attrs":{},"tags":{},"state":"pending"}`+"\n")
		flusher.Flush()
		close(started)
		<-r.Context().Done()
		close(release)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got int
	err = client.ExportJobs(ctx, func(job httpdto.JobResponse) error {
		got++
		if job.Name != "first" {
			t.Errorf("job = %q, want first", job.Name)
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ExportJobs error = %v, want context.Canceled", err)
	}
	if got != 1 {
		t.Fatalf("callback called %d times, want once", got)
	}
	<-started
	select {
	case <-release:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not observe cancellation")
	}
}

func TestGetJobResponseContract(t *testing.T) {
	client := &Client{
		baseURL: "http://scheduler",
		httpClient: &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodGet || r.URL.Path != "/jobs/42" {
					t.Errorf("request = %s %s, want GET /jobs/42", r.Method, r.URL.Path)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body: io.NopCloser(strings.NewReader(
						`{"id":42,"name":"job","description":"details","complexity":2,"attrs":{"env":"test"},"tags":{"batch":{}},"state":"running"}`,
					)),
					Header:  make(http.Header),
					Request: r,
				}, nil
			}),
		},
	}

	got, err := client.GetJob(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	want := httpdto.JobResponse{
		ID:          42,
		Name:        "job",
		Description: "details",
		Complexity:  2,
		Attrs:       map[string]string{"env": "test"},
		Tags:        map[string]struct{}{"batch": {}},
		State:       "running",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("job response = %#v, want %#v", got, want)
	}
}
