package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// нет гарантии порядка выполнения горутин
func TestVisibility(t *testing.T) {
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

func TestVisibilityMutex(t *testing.T) {
	mu := sync.Mutex{}
	var ready bool
	var value int
	go func() {
		mu.Lock()
		ready = true
		value = 42
		mu.Unlock()
	}()
	go func() {
		for {
			mu.Lock()
			if ready {
				fmt.Println(value)
				mu.Unlock()
				break
			}
			mu.Unlock()
		}
	}()
}

func TestBufferedChannel(t *testing.T) {
	ch := make(chan int, 1)
	var value int
	go func() {
		value = 100
		ch <- 1
	}()
	go func() {
		<-ch // тут чтение из канала работает как блокировка
		fmt.Println(value)
	}()
}

// атомик не защищает поля. горутиын могут в разном порядке пойти
func TestAtomic(t *testing.T) {
	type Config struct {
		A int
		B int
	}
	var ptr atomic.Pointer[Config]
	cfg := &Config{A: 1, B: 2}
	ptr.Store(cfg)
	go func() {
		ptr.Load()
		cfg.A++
		cfg.B++
	}()
	go func() {
		c := ptr.Load()
		fmt.Println(c.A, c.B)
	}()
}

func BenchmarkFalseSharing(b *testing.B) {
	b.Run("no padding", func(b *testing.B) {
		type Metrics struct {
			A atomic.Uint64
			B atomic.Uint64
		}
		m := Metrics{}
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				m.A.Add(1)
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				m.B.Add(1)
			}
		}()

		wg.Wait()
	})
	b.Run("padding", func(b *testing.B) {
		type Metrics struct {
			A atomic.Uint64
			_ [56]byte
			B atomic.Uint64
		}
		m := Metrics{}
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				m.A.Add(1)
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 1_000_000; i++ {
				m.B.Add(1)
			}
		}()

		wg.Wait()
	})
}
