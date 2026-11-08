package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"playground/in-memory-scheduler/internal/httpdto"
)

func writeJSON(w http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		log.Printf("marshal HTTP response: %v", err)

		status = http.StatusInternalServerError
		data = []byte(`{"error":"internal server error"}`)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeJSONError(
	w http.ResponseWriter,
	status int,
	message string,
) {
	writeJSON(w, status, httpdto.ErrorResponse{
		Error: message,
	})
}

func writeSchedulerError(w http.ResponseWriter, err error) {
	status := ToHTTPStatus(err)
	writeJSONError(w, status, http.StatusText(status))
}

func writeBodyError(w http.ResponseWriter, err error) {
	var maxBytesErr *http.MaxBytesError

	if errors.As(err, &maxBytesErr) {
		writeJSONError(
			w,
			http.StatusRequestEntityTooLarge,
			"request body too large",
		)
		return
	}

	writeJSONError(
		w,
		http.StatusBadRequest,
		"invalid JSON body",
	)
}
