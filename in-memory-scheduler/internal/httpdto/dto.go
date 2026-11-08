package httpdto

// CreateJobRequest — входящий запрос на создание задачи.
type CreateJobRequest struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Complexity  *int                `json:"complexity"`
	Attrs       map[string]string   `json:"attrs"`
	Tags        map[string]struct{} `json:"tags"`
}

// CreateJobResponse — ответ POST /jobs.
// ID пока не возвращаем, поскольку Scheduler.Add возвращает только error.
type CreateJobResponse struct {
	Name string `json:"name"`
}

// JobResponse — публичное представление задачи.
type JobResponse struct {
	ID          int                 `json:"id"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Complexity  int                 `json:"complexity"`
	Attrs       map[string]string   `json:"attrs"`
	Tags        map[string]struct{} `json:"tags"`
	State       string              `json:"state"`
}

// ErrorResponse — единый формат HTTP-ошибок.
type ErrorResponse struct {
	Error string `json:"error"`
}

// StatsResponse — публичное представление статистики.
type StatsResponse struct {
	Done      uint `json:"done"`
	Failed    uint `json:"failed"`
	Cancelled uint `json:"cancelled"`
}
