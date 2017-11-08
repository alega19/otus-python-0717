package main

import (
	"github.com/rainycape/memcache"
	"time"
	"sync"
)

type MemcacheClient struct {
	c     *memcache.Client
	mu    sync.Mutex
	msgCh chan *Message
}

func NewMemcacheClient(ip string, timeout time.Duration) (mc *MemcacheClient, err error) {
	mc = &MemcacheClient{}
	mc.c, err = memcache.New(ip); if err != nil {return}
	mc.c.SetTimeout(timeout)
	mc.msgCh = make(chan *Message, 64)
	go mc.startLoop()
	return
}

func (mc *MemcacheClient) startLoop() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for msg := range mc.msgCh {
		var err error
		for i := 0; i < 3; i++ {
			err = mc.c.Set(msg.item)
			if err == nil {
				break
			}
		}
		if err != nil {
			logger.Println(err)
			msg.errCh <- true
		} else {
			msg.errCh <- false
		}
	}
}

func (mc *MemcacheClient) Close() {
	close(mc.msgCh)
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.c.Close()
}

func (mc *MemcacheClient) Set(msg *Message) {
	mc.msgCh <- msg
	return
}
