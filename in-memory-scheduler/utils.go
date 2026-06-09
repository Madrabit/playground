package main

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

func ShortDescription(s string) string {
	r := []rune(s)
	if len(r) >= 10 {
		return string(r[:10])
	}
	return s
}

func NameLength(name string) int {
	return utf8.RuneCountInString(name)
}

func ValidateJob(j Job) error {
	if j.ID <= 0 {
		return &ValidationError{
			Field:  "ID",
			Value:  j.ID,
			Reason: "must be greater than 0",
		}
	}
	if strings.TrimSpace(j.Name) == "" {
		return &ValidationError{
			Field:  "Name",
			Value:  j.Name,
			Reason: "Invalid Job Name",
		}
	}
	if utf8.RuneCountInString(j.Name) > 32 {
		return &ValidationError{
			Field:  "Name",
			Value:  j.Name,
			Reason: "name exceeds 32 characters",
		}
	}

	if j.Complexity > 10 || j.Complexity <= 0 {
		return &ValidationError{
			Field:  "Complexity",
			Value:  j.Complexity,
			Reason: "must be between 1 and 10",
		}
	}
	if len(j.Tags) > 3 {
		return &ValidationError{
			Field:  "Tags",
			Value:  len(j.Tags),
			Reason: "must be no more than 3 tags",
		}
	}
	for k := range j.Tags {
		if strings.TrimSpace(k) == "" {
			return &ValidationError{
				Field:  "Tags",
				Value:  k,
				Reason: "tag cannot be blank",
			}
		}
	}
	return nil
}

type ValidationError struct {
	Field  string
	Value  any
	Reason string
}

func (v *ValidationError) Error() string {
	return fmt.Sprintf("%s (value=%v): %s", v.Field, v.Value, v.Reason)
}
