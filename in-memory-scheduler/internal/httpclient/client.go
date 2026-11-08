package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"playground/in-memory-scheduler/internal/httpdto"
	"strconv"
	"strings"
	"time"
)

const (
	maxResponseBodyBytes = 4 << 20 // 4 MiB
	maxErrorBodyBytes    = 8 << 10 // 8 KiB
)

// Client хранит один переиспользуемый HTTP-клиент.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

type CreateJobRequest struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Complexity  int                 `json:"complexity"`
	Attrs       map[string]string   `json:"attrs"`
	Tags        map[string]struct{} `json:"tags"`
}

// Ответ существующего POST /jobs.
type CreateJobResponse struct {
	Name string `json:"name"`
}

// Ответ существующего GET /stats.
type StatsResponse struct {
	Done      uint `json:"done"`
	Failed    uint `json:"failed"`
	Cancelled uint `json:"cancelled"`
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf(
			"scheduler API returned HTTP %d",
			e.StatusCode,
		)
	}

	return fmt.Sprintf(
		"scheduler API returned HTTP %d: %s",
		e.StatusCode,
		e.Body,
	)
}

func NewClient(baseURL string, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		return nil, errors.New("HTTP client timeout must be positive")
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	if (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return nil, errors.New("invalid scheduler API base URL")
	}

	// Клонируем стандартный Transport, чтобы получить
	// собственный пул соединений с настройками по умолчанию.
	transport := http.DefaultTransport.(*http.Transport).Clone()

	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 10
	transport.IdleConnTimeout = 90 * time.Second

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: client,
	}, nil
}

// Освобождает простаивающие соединения.
// Вызывать при завершении работы клиента.
func (c *Client) CloseIdleConnections() {
	c.httpClient.CloseIdleConnections()
}

// Создание задачи.
func (c *Client) CreateJob(
	ctx context.Context,
	job CreateJobRequest,
) (CreateJobResponse, error) {
	var response CreateJobResponse

	err := c.doJSON(
		ctx,
		http.MethodPost,
		"/jobs",
		job,
		&response,
	)

	return response, err
}

// Получение задачи по ID.
func (c *Client) GetJob(
	ctx context.Context,
	id int,
) (httpdto.JobResponse, error) {
	var job httpdto.JobResponse

	if id <= 0 {
		return job, errors.New("job ID must be positive")
	}

	err := c.doJSON(
		ctx,
		http.MethodGet,
		"/jobs/"+strconv.Itoa(id),
		nil,
		&job,
	)

	return job, err
}

// Получение статистики.
func (c *Client) GetStats(
	ctx context.Context,
) (StatsResponse, error) {
	var stats StatsResponse

	err := c.doJSON(
		ctx,
		http.MethodGet,
		"/stats",
		nil,
		&stats,
	)

	return stats, err
}

// Общий метод выполнения HTTP-запросов.
func (c *Client) doJSON(
	ctx context.Context,
	method string,
	path string,
	requestBody any,
	responseBody any,
) error {
	var body io.Reader

	if requestBody != nil {
		data, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}

		body = bytes.NewReader(data)
	}

	// Контекст передаётся непосредственно HTTP-запросу.
	req, err := http.NewRequestWithContext(
		ctx,
		method,
		c.baseURL+path,
		body,
	)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	// Используем один и тот же http.Client.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}

	// Читаем и обязательно закрываем response body.
	data, err := readAndCloseResponse(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(data))

		if len(message) > maxErrorBodyBytes {
			message = message[:maxErrorBodyBytes]
		}

		return &HTTPError{
			StatusCode: resp.StatusCode,
			Body:       message,
		}
	}

	if responseBody == nil {
		return nil
	}

	if len(data) == 0 {
		return errors.New("scheduler API returned empty response")
	}

	if err := json.Unmarshal(data, responseBody); err != nil {
		return fmt.Errorf("decode response JSON: %w", err)
	}

	return nil
}

// Читает ограниченное количество байтов и закрывает body.
func readAndCloseResponse(body io.ReadCloser) ([]byte, error) {
	defer body.Close()

	data, err := io.ReadAll(
		io.LimitReader(body, maxResponseBodyBytes+1),
	)
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > maxResponseBodyBytes {
		return nil, errors.New("response body exceeds size limit")
	}

	return data, nil
}
