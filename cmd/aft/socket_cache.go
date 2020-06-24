package main

import (
	"sync"

	zmq "github.com/pebbe/zmq4"
)

func createSocket(context *zmq.Context, address string) *zmq.Socket {
	sckt, err := context.NewSocket(zmq.PUSH)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}

	err = sckt.Connect(address)
	if err != nil {
		fmt.Printf("Unexpected error while binding/connecting socket to %s:\n%v", address, err)
		os.Exit(1)
	}

	return sckt
}

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

func (socketCache *SocketCache) Get(string address) *zmq.Socket {
	socketCache.updateMutex.Lock()
	if socket, ok := socketCache.cache[address]; ok {
		socketCache.updateMutex.Unlock()
		return socket
	}

	socket := createSocket(socketCache.context, address)
	socketCache[address] = socket
	socketCache.Unlock()
	return socket
}
