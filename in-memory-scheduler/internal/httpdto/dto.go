package httpdto

// CreateJobRequest — входящий запрос на создание задачи.
// Name и Description — значения: отсутствие и пустая строка имеют одинаковый смысл.
// Complexity — указатель: nil (отсутствие/null) отличается от явно переданного 0.
// omitempty при отправке запроса опускает nil, но сохраняет указатель на 0.
// ID, State и внутренние поля Job не принимаются от клиента.
type CreateJobRequest struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Complexity  *int                `json:"complexity,omitempty"`
	Attrs       map[string]string   `json:"attrs"`
	Tags        map[string]struct{} `json:"tags"`
}

// CreateJobResponse — ответ POST /jobs.
// ID пока не возвращаем, поскольку Scheduler.Add возвращает только error.
type CreateJobResponse struct {
	Name string `json:"name"`
}

// JobResponse — публичное представление задачи.
// Все поля присутствуют даже при zero value; пустые Attrs и Tags передаются как {}.
// State содержит стабильное имя состояния: pending, running, failed, done, cancelled или stopped.
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
// Нулевые счётчики передаются явно, поэтому omitempty не используется.
type StatsResponse struct {
	Done      uint `json:"done"`
	Failed    uint `json:"failed"`
	Cancelled uint `json:"cancelled"`
}
