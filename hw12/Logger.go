package main

import (
	"log"
	"sync"
)

type Logger struct {
	Ch chan interface{}
	mu sync.Mutex
}

func NewLogger() (lg *Logger) {
	lg = &Logger{
		Ch: make(chan interface{}),
	}
	go lg.startLoop()
	return
}

func (lg *Logger) startLoop() {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	for v := range lg.Ch {
		log.Println(v)
	}
}

func (lg *Logger) TryStopAndWait() {
	close(lg.Ch)
	lg.mu.Lock()
	lg.mu.Unlock()
}

func (lg *Logger) Println(v interface{}) {
	lg.Ch <- v
}
