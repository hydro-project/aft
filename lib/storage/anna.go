package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"

	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	TransactionsKey = "Transactions"
)

type AnnaStorageManager struct {
	freeClients []*AnnaClient
	clientLock  *sync.Mutex
}

func NewAnnaStorageManager(ipAddress string, elbAddress string) *AnnaStorageManager {
	clients := []*AnnaClient{}
	for i := 0; i < 4; i++ {
		anna := NewAnnaClient(elbAddress, ipAddress, false, i)
		clients = append(clients, anna)
	}

	return &AnnaStorageManager{
		freeClients: clients,
		clientLock:  &sync.Mutex{},
	}
}

func (anna *AnnaStorageManager) getClient() *AnnaClient {
	// We don't need to wait for clients because there will only ever by 3 client
	// threads that operate per-machine.
	anna.clientLock.Lock()
	client := anna.freeClients[0]
	anna.freeClients = anna.freeClients[1:]
	anna.clientLock.Unlock()

	return client
}

func (anna *AnnaStorageManager) releaseClient(client *AnnaClient) {
	anna.clientLock.Lock()
	anna.freeClients = append(anna.freeClients, client)
	anna.clientLock.Unlock()
}

func (anna *AnnaStorageManager) StartTransaction(id string) error {
	return nil
}

func (anna *AnnaStorageManager) CommitTransaction(transaction *pb.TransactionRecord) error {
	key := fmt.Sprintf(TransactionKey, transaction.Id, transaction.Timestamp)
	serialized, err := proto.Marshal(transaction)
	if err != nil {
		return err
	}

	client := anna.getClient()
	defer anna.releaseClient(client)
	_, err = client.Put(key, serialized)

	if err != nil {
		return err
	}

	// Add this transaction key to the set of committed transactions.
	// TODO: This set becomes big really fast. Need to figure out how to
	// optimize it.
	txns, _ := client.GetSet(TransactionsKey)

	txns = append(txns, key)
	_, err = client.PutSet(TransactionsKey, txns)

	return err
}

func (anna *AnnaStorageManager) AbortTransaction(transaction *pb.TransactionRecord) error {
	return nil
}

func (anna *AnnaStorageManager) Get(key string) (*pb.KeyValuePair, error) {
	result := &pb.KeyValuePair{}

	client := anna.getClient()
	defer anna.releaseClient(client)
	bts, err := client.Get(key)
	for err != nil && strings.Contains(err, "KEY_DNE") {
		bts, err := client.Get(key)
	}

	if err != nil {
		return result, err
	}

	err = proto.Unmarshal(bts, result)
	return result, err
}

func (anna *AnnaStorageManager) GetTransaction(transactionKey string) (*pb.TransactionRecord, error) {
	result := &pb.TransactionRecord{}

	client := anna.getClient()
	defer anna.releaseClient(client)
	bts, err := client.Get(transactionKey)
	if err != nil {
		return result, err
	}

	err = proto.Unmarshal(bts, result)
	return result, err
}

func (anna *AnnaStorageManager) MultiGetTransaction(transactionKeys *[]string) (*[]*pb.TransactionRecord, error) {
	results := make([]*pb.TransactionRecord, len(*transactionKeys))

	for index, key := range *transactionKeys {
		txn, err := anna.GetTransaction(key)
		if err != nil {
			return &[]*pb.TransactionRecord{}, err
		}

		results[index] = txn
	}

	return &results, nil
}

func (anna *AnnaStorageManager) Put(key string, val *pb.KeyValuePair) error {
	serialized, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	client := anna.getClient()
	defer anna.releaseClient(client)
	_, err = client.Put(key, serialized)
	return err
}

func (anna *AnnaStorageManager) MultiPut(data *map[string]*pb.KeyValuePair) error {
	for key, val := range *data {
		err := anna.Put(key, val)
		if err != nil {
			fmt.Printf("Writing %s. ERROR: %v\n", key, err)
			return err
		}
	}

	return nil
}

func (anna *AnnaStorageManager) Delete(key string) error {
	return nil // Anna does not support deletes.
}

func (anna *AnnaStorageManager) MultiDelete(keys *[]string) error {
	return nil // Anna does not support deletes.
}

func (anna *AnnaStorageManager) List(prefix string) ([]string, error) {
	if prefix != "transactions" {
		return nil, errors.New(fmt.Sprintf("Unexpected prefix: %s", prefix))
	}

	client := anna.getClient()
	defer anna.releaseClient(client)
	return client.GetSet(TransactionsKey)
}
