package main

import (
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
