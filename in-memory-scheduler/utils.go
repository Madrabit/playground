package main

import (
	"fmt"
	"runtime"
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

func dumpStacks() {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	fmt.Printf("=== goroutine dump ===\n%s\n", buf[:n])
}
