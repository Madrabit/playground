package main

import "fmt"

func main() {
	var ready bool
	var value int
	go func() {
		ready = true
		value = 42
	}()
	go func() {
		for !ready {

		}
		fmt.Println(value)
	}()
}
