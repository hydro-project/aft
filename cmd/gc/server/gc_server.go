package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"

	"github.com/vsreekanti/aft/config"
	"github.com/vsreekanti/aft/lib/consistency"
	"github.com/vsreekanti/aft/lib/storage"
	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	PullTemplate = "tcp://*:%d"

	GcPort = 7782
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

func main() {
	conf := config.ParseConfig()

	var consistencyManager consistency.ConsistencyManager
	switch conf.ConsistencyType {
	case "lww":
		consistencyManager = &consistency.LWWConsistencyManager{}
	case "read-atomic":
		consistencyManager = &consistency.ReadAtomicConsistencyManager{}
	}

	var storageManager storage.StorageManager
	switch conf.StorageType {
	case "s3":
		storageManager = storage.NewS3StorageManager("vsreekanti")
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("AftData", "AftData")
	case "redis":
		storageManager = storage.NewRedisStorageManager("aft-test.kxmfgs.clustercfg.use1.cache.amazonaws.com:6379", "")
	}

	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	deletePullSocket := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, GcPort), true)
	poller := zmq.NewPoller()
	poller.Add(deletePullSocket, zmq.POLLIN)

	reportStart := time.Now()
	txnDeleteCount := 0
	txnDeleteLock := &sync.Mutex{}
	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		// We only have 1 socket, so no need to check each.
		if len(sockets) > 0 {
			bts, _ := deletePullSocket.RecvBytes(zmq.DONTWAIT)

			txnList := &pb.TransactionList{}
			err = proto.Unmarshal(bts, txnList)
			if err != nil {
				fmt.Println("Unable to parse received TransactionIdList:\n", err)
				continue
			}

			batchSize := 25
			batch := []*pb.TransactionRecord{}
			for _, transaction := range txnList.Records {
				batch = append(batch, transaction)

				if len(batch) == batchSize {
					go deleteTransactions(batch, &storageManager, &consistencyManager, &txnDeleteCount, txnDeleteLock)
					batch = []*pb.TransactionRecord{}
				}
			}

			go deleteTransactions(batch, &storageManager, &consistencyManager, &txnDeleteCount, txnDeleteLock)
		}

		reportEnd := time.Now()
		if reportEnd.Sub(reportStart).Seconds() > 1.0 {
			txnDeleteLock.Lock()
			fmt.Printf("%s: Deleted %d transactions in the last second.\n", time.Now().String(), txnDeleteCount)
			txnDeleteCount = 0
			txnDeleteLock.Unlock()

			reportStart = time.Now()
		}
	}
}

func deleteTransactions(
	batch []*pb.TransactionRecord,
	storageMgr *storage.StorageManager,
	cm *consistency.ConsistencyManager,
	txnDeleteCount *int,
	txnDeleteLock *sync.Mutex,
) {
	keys := []string{}

	for _, transaction := range batch {
		for _, key := range transaction.WriteSet {
			storageKey := (*cm).GetStorageKeyName(key, transaction.Timestamp, transaction.Id)
			keys = append(keys, storageKey)
		}

		txnKey := fmt.Sprintf(storage.TransactionKey, transaction.Id, transaction.Timestamp)
		keys = append(keys, txnKey)
	}

	(*storageMgr).MultiDelete(&keys)

	txnDeleteLock.Lock()
	*txnDeleteCount += len(batch)
	txnDeleteLock.Unlock()
}
