package httpapi

type CreateJobReq struct {
	Complexity  int                 `json:"complexity"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Tags        map[string]struct{} `json:"tags"`
	SomeSlice   []string            `json:"someSlice"`
	Attrs       map[string]string   `json:"attrs"`
}

type CreateJobResp struct {
	Name string `json:"name"`
}
