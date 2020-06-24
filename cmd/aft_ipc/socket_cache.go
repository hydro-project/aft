package main

import (
	"sync"

	zmq "github.com/pebbe/zmq4"
)

type SocketCache struct {
	context     *zmq.Context
	updateMutex *sync.Mutex
	cache       map[string]*zmq.Socket
}

func NewSocketCache(context *zmq.Context) *SocketCache {
	return &SocketCache{
		context:     context,
		updateMutex: &sync.Mutex{},
		cache:       map[string]*zmq.Socket{},
	}
}

func (socketCache *SocketCache) Get(address string) *zmq.Socket {
	socketCache.updateMutex.Lock()
	if socket, ok := socketCache.cache[address]; ok {
		socketCache.updateMutex.Unlock()
		return socket
	}

	socket := createSocket(zmq.PUSH, socketCache.context, address, false)
	socketCache.cache[address] = socket
	socketCache.updateMutex.Unlock()
	return socket
}
