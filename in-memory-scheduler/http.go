package main

import (
	"errors"
	"net/http"
)

func ToHTTPStatus(err error) int {

	if errors.Is(err, ErrDuplicateJob) {
		return http.StatusConflict
	}
	if errors.Is(err, ErrJobNotFound) {
		return http.StatusNotFound
	}
	var ve *ValidationError
	if errors.As(err, &ve) {
		return http.StatusBadRequest
	}
	if err == nil {
		return http.StatusOK
	}
	return http.StatusInternalServerError
}
