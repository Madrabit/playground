package main

import (
	"fmt"
	"testing"
	"time"
)

func TestDoubleGoroutineRecover(t *testing.T) {
	go func() {
		panic("boom 1")
	}()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("caught: ", r)
			}
		}()
		panic("boom 2")
	}()
	go func() {
		defer recover()
		panic("boom 3")
	}()
	time.Sleep(10 * time.Second)
}
func a() {
	defer fmt.Println("A")
	b()
}

func b() {
	defer fmt.Println("B")
	c()
}

func c() {
	defer fmt.Println("C")
	panic("boom 1")
}

func TestStackUnwinding(t *testing.T) {
	a()
}

/*
не найден файл error

ошибка JSON пользователя error

деление на ноль внутри библиотеки panic так как невозможное состояние

невалидная конфигурация - panic потому что конфиг нужен при старте приложения и лучше егос разу сложить паникой

ошибка подключения к Postgres - подключение на старте приложения происходит и она критична. лучше panic

невозможное состояние state machine  panic так как невозможное состояние

ошибка сети error

nil pointer в коде* - panic ошибка программиста

*/

func TestSwallowPanic(t *testing.T) {
	defer func() {
		recover() // проглотили
	}()
	panic("boom")
}

func TestLogPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("caught:", r)
		}
	}()
	panic("boom")
}

func TestRepanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			panic(r) // снова кидаем
		}
	}()
	panic("boom")
}
