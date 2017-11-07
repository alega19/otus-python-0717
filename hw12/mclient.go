package main

import (
	"github.com/rainycape/memcache"
	"time"
	"sync"
)

type MemcacheClient struct {
	c *memcache.Client
	mu sync.Mutex
	itemsCh chan *memcache.Item
}

func NewMemcacheClient(ip string, timeout time.Duration) (mc *MemcacheClient, err error) {
	mc = &MemcacheClient{}
	mc.c, err = memcache.New(ip); if err != nil {return}
	mc.c.SetTimeout(timeout)
	mc.itemsCh = make(chan *memcache.Item, 64)
	go mc.startLoop()
	return
}

func (mc *MemcacheClient) startLoop() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for item := range mc.itemsCh {
		var err error
		for i := 0; i < 3; i++ {
			err = mc.c.Set(item)
			if err == nil {
				break
			}
		}
		if err != nil {
			logger.Println(err)
		}
	}
}

func (mc *MemcacheClient) Close() {
	close(mc.itemsCh)
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.c.Close()
}

func (mc *MemcacheClient) Set(item *memcache.Item) {
	mc.itemsCh <- item
	return
}
