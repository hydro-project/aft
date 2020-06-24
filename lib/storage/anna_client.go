package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"

	annapb "github.com/vsreekanti/aft/proto/anna"
)

type AnnaClient struct {
	elbAddress         string
	ipAddress          string
	responseAddress    string
	keyResponseAddress string
	tid                int
	local              bool
	context            *zmq.Context
	addressCache       map[string][]string
	responsePuller     *zmq.Socket
	keyAddressPuller   *zmq.Socket
	socketCache        map[string]*zmq.Socket
}

const (
	AddressTemplate = "tcp://%s:%d"
)

func createSocket(tp zmq.Type, context *zmq.Context, address string, bind bool) *zmq.Socket {
	sckt, err := context.NewSocket(tp)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}

	if bind {
		err = sckt.Bind(address)
	} else {
		err = sckt.Connect(address)
	}

	if err != nil {
		fmt.Printf("Unexpected error while binding/connecting socket to %s:\n%v", address, err)
		os.Exit(1)
	}

	return sckt
}

func NewAnnaClient(elbAddress string, ipAddress string, local bool, tid int) *AnnaClient {
	context, err := zmq.NewContext()

	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	responsePullerAddress := fmt.Sprintf("tcp://*:%d", 6460+tid)
	keyResponsePullerAddress := fmt.Sprintf("tcp://*:%d", 6760+tid)

	responsePuller := createSocket(zmq.PULL, context, responsePullerAddress, true)
	keyAddressPuller := createSocket(zmq.PULL, context, keyResponsePullerAddress, true)

	responseAddress := fmt.Sprintf("tcp://%s:%d", ipAddress, 6460+tid)
	keyResponseAddress := fmt.Sprintf("tcp://%s:%d", ipAddress, 6760+tid)

	return &AnnaClient{
		elbAddress:         elbAddress,
		ipAddress:          ipAddress,
		tid:                tid,
		local:              local,
		context:            context,
		addressCache:       map[string][]string{},
		responsePuller:     responsePuller,
		keyAddressPuller:   keyAddressPuller,
		responseAddress:    responseAddress,
		keyResponseAddress: keyResponseAddress,
		socketCache:        map[string]*zmq.Socket{},
	}
}

func (anna *AnnaClient) get(key string) (*annapb.KeyTuple, error) {
	workers := anna.getWorkerAddresses(key)

	for len(workers) == 0 {
		fmt.Printf("%v: No workers found for key %s.\n", time.Now(), key)
		workers = anna.getWorkerAddresses(key)
	}

	worker := workers[rand.Intn(len(workers))]
	socket := anna.getSocket(worker)

	request := anna.prepareDataRequest(key)
	request.Type = annapb.RequestType_GET

	bts, _ := proto.Marshal(request)
	socket.SendBytes(bts, zmq.DONTWAIT)

	response := &annapb.KeyResponse{}
	bts, _ = anna.responsePuller.RecvBytes(0)
	proto.Unmarshal(bts, response)

	// We only support one GET/PUT at a time.
	tuple := response.Tuples[0]
	if tuple.Error == annapb.AnnaError_NO_ERROR && response.Error == annapb.AnnaError_NO_ERROR {
		return tuple, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Anna Error: %s, %s", response.Error, tuple.Error))
	}
}

func (anna *AnnaClient) Get(key string) ([]byte, error) {
	tuple, err := anna.get(key)

	if err != nil {
		return nil, err
	}

	if tuple.LatticeType != annapb.LatticeType_LWW {
		return nil, errors.New(fmt.Sprintf("Unsupported LatticeType: %s", tuple.LatticeType))
	}

	value := &annapb.LWWValue{}
	proto.Unmarshal(tuple.Payload, value)

	return value.Value, nil
}

func (anna *AnnaClient) GetSet(key string) ([]string, error) {
	tuple, err := anna.get(key)

	if err != nil {
		return nil, err
	}

	if tuple.LatticeType != annapb.LatticeType_SET {
		return nil, errors.New(fmt.Sprintf("Unsupported LatticeType: %s", tuple.LatticeType))
	}

	value := &annapb.SetValue{}
	proto.Unmarshal(tuple.Payload, value)

	result := []string{}
	for _, val := range value.Values {
		sval := string(val)
		result = append(result, sval)
	}

	return result, nil
}

func (anna *AnnaClient) put(key string, request *annapb.KeyRequest) (bool, error) {
	workers := anna.getWorkerAddresses(key)

	if len(workers) == 0 {
		return false, errors.New(fmt.Sprintf("No workers found for key %s.", key))
	}

	worker := workers[rand.Intn(len(workers))]
	socket := anna.getSocket(worker)

	bts, _ := proto.Marshal(request)
	socket.SendBytes(bts, zmq.DONTWAIT)

	response := &annapb.KeyResponse{}
	bts, _ = anna.responsePuller.RecvBytes(0)
	proto.Unmarshal(bts, response)

	// We only support one GET/PUT at a time.
	tuple := response.Tuples[0]
	if tuple.Error == annapb.AnnaError_NO_ERROR {
		return true, nil
	} else {
		return false, errors.New(fmt.Sprintf("Anna Error: %s", tuple.Error))
	}
}

func (anna *AnnaClient) Put(key string, value []byte) (bool, error) {
	data := &annapb.LWWValue{}
	data.Value = value
	data.Timestamp = 10

	payload, _ := proto.Marshal(data)

	request := anna.prepareDataRequest(key)
	request.Type = annapb.RequestType_PUT
	request.Tuples[0].Payload = payload
	request.Tuples[0].LatticeType = annapb.LatticeType_LWW

	return anna.put(key, request)
}

func (anna *AnnaClient) PutSet(key string, values []string) (bool, error) {
	data := &annapb.SetValue{}
	for _, val := range values {
		bval := []byte(val)
		data.Values = append(data.Values, bval)
	}

	payload, _ := proto.Marshal(data)

	request := anna.prepareDataRequest(key)
	request.Type = annapb.RequestType_PUT
	request.Tuples[0].Payload = payload
	request.Tuples[0].LatticeType = annapb.LatticeType_SET

	return anna.put(key, request)
}

func (anna *AnnaClient) getWorkerAddresses(key string) []string {
	if addresses, ok := anna.addressCache[key]; ok {
		if len(addresses) > 0 {
			return addresses
		}
	}
	addresses := anna.queryRouting(key)
	anna.addressCache[key] = addresses
	return addresses
}

func (anna *AnnaClient) queryRouting(key string) []string {
	request := &annapb.KeyAddressRequest{}
	request.ResponseAddress = anna.keyResponseAddress
	request.Keys = append(request.Keys, key)

	port := 6450
	if !anna.local {
		port += rand.Intn(4)
	}

	destination := fmt.Sprintf("tcp://%s:%d", anna.elbAddress, port)
	socket := anna.getSocket(destination)

	bts, _ := proto.Marshal(request)
	socket.SendBytes(bts, zmq.DONTWAIT)

	response := &annapb.KeyAddressResponse{}
	bts, _ = anna.keyAddressPuller.RecvBytes(0)

	proto.Unmarshal(bts, response)

	if response.Error != annapb.AnnaError_NO_ERROR {
		return []string{}
	}

	return response.Addresses[0].Ips // We only request one key at a time.
}

func (anna *AnnaClient) getSocket(address string) *zmq.Socket {
	if socket, ok := anna.socketCache[address]; ok {
		return socket
	}

	socket := createSocket(zmq.PUSH, anna.context, address, false)
	anna.socketCache[address] = socket
	return socket
}

func (anna *AnnaClient) prepareDataRequest(key string) *annapb.KeyRequest {
	request := &annapb.KeyRequest{}
	request.ResponseAddress = anna.responseAddress

	tuple := &annapb.KeyTuple{}
	tuple.Key = key

	request.Tuples = append(request.Tuples, tuple)

	return request
}
