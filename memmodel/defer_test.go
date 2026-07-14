package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

// вывод 1
func TestDeferArgs(t *testing.T) {
	x := 1

	defer fmt.Println(x)

	x = 2
}

// вывод 1
func TestDeferArgsSecond(t *testing.T) {
	x := 1
	defer func(v int) {
		fmt.Println(v)
	}(x)

	x = 3
}

// вывод 3 не передаем как параметр и нет захвата значения до завершения функции
func TestDeferArgs3d(t *testing.T) {
	x := 1
	defer func() {
		fmt.Println(x)
	}()
	x = 3
}

// деферы в обратном порядке работают, как разбор стека вывод 4 3 2 1
func TestDeferLIFO(t *testing.T) {
	defer fmt.Println("1")
	defer fmt.Println("2")
	defer fmt.Println("3")
	defer fmt.Println("4")
}

func f() int {
	defer fmt.Println("defer")
	return 10
}

// напечатало defer потом 10. что логично. мы вернули 10 из функции и только потом использовали это значение для печати
func TestDeferReturn(t *testing.T) {
	fmt.Println(f())
}

func f2() (x int) {
	defer func() {
		x++
	}()
	return 10
}

// возвращает 10, а потом появляется под конец переменная x  и происходит инкремент. вывод 11
func TestDeferReturn2(t *testing.T) {
	fmt.Println(f2())
}

func f3() int {
	x := 10
	defer func() {
		x++
	}()
	return x
}

// 10 возвращает реально из функции. то что дефер сделает инкремент уже возвращаемое значение не изменит
func TestDeferReturn3(t *testing.T) {
	fmt.Println(f3())
}

var diskErr = errors.New("disk error")

func Save() (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("save error: %w", err)
		}
	}()

	return diskErr
}

func TestSaveDefer(t *testing.T) {
	err := Save()
	if !errors.Is(err, diskErr) {
		t.Fatalf("expected wrapped disk error, got %v", err)
	}
	fmt.Println(err)
}

func fPanic() {
	defer fmt.Println("cleanup")

	panic("boom")
}

func TestDeferPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println("panic: ", r)
		}
	}()
	fPanic()
}

func TestDeferLoop(t *testing.T) {
	//for i := 0; i < 5; i++ {
	//	defer fmt.Println(i)
	//}
	for i := 0; i < 5; i++ {
		f, _ := os.CreateTemp("", fmt.Sprintf("%d.txt", i))
		defer f.Close()
	}

	for i := 0; i < 5; i++ {
		f, _ := os.CreateTemp("", fmt.Sprintf("%d.txt", i))
		err := f.Close()
		if err != nil {
			return
		}
	}

	for i := 0; i < 5; i++ {
		func(i int) {
			f, _ := os.CreateTemp("", fmt.Sprintf("%d.txt", i))
			defer func(f *os.File) {
				err := f.Close()
				if err != nil {
					return
				}
			}(f)
		}(i)
	}
}

func BenchmarkMutexDefer(b *testing.B) {
	var mu sync.Mutex
	for i := 0; i < b.N; i++ {
		func() {
			mu.Lock()
			defer mu.Unlock()
		}()
	}
}

func BenchmarkMutexExplicit(b *testing.B) {
	var mu sync.Mutex
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}
