package storage

import (
	pb "github.com/vsreekanti/aft/proto/aft"
)

const TransactionKey = "transactions/%s-%d"

type StorageManager interface {
	// Start a new transaction with the execution ID passed in; this ID will be
	// used for all operations relevant to this particular transaction.
	StartTransaction(id string) error

	// Commit all of the changes made in this transaction to the storage engine.
	CommitTransaction(transaction *pb.TransactionRecord) error

	// Abort all of the changes made in this transaction, so none of them will be
	// persisted in the storage engine. NOTE: For now, all `StorageManager`s do
	// nothing in `AbortTransaction` because we do not eagerly persist keys. If
	// we ever change that design, we will need to revisit this and ensure to
	// clean up after the aborted transaction.
	AbortTransaction(transaction *pb.TransactionRecord) error

	// Retrieve a transaction record from the storage engine.
	GetTransaction(transactionKey string) (*pb.TransactionRecord, error)

	// Retrieve a set of transactions from the storage engine.
	MultiGetTransaction(transactionKeys *[]string) (*[]*pb.TransactionRecord, error)

	// As a part of the transaction owned by tid, insert a key-value pair into
	// the storage engine.
	Put(key string, val *pb.KeyValuePair) error

	// As a part of transaction owned by tid, insert a set of key-value pairs
	// into the storage engine.
	MultiPut(*map[string]*pb.KeyValuePair) error

	// Retrieve the given key as a part of the transaction tid.
	Get(key string) (*pb.KeyValuePair, error)

	// Returns a list of the keys that start with the given prefix.
	List(prefix string) ([]string, error)

	// Deletes the given key from the underlying storage engine.
	Delete(key string) error

	// Delete multiple keys at once.
	MultiDelete(*[]string) error
}
