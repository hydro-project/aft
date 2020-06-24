package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"

	"github.com/vsreekanti/aft/config"
	"github.com/vsreekanti/aft/lib/consistency"
	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	PushTemplate = "tcp://%s:%d"
	PullTemplate = "tcp://*:%d"

	// Ports to notify and be notified of new transactions.
	TxnPort = 7777

	// Ports to notify and be notified of pending transaction deletes.
	PendingTxnDeletePushPort = 7779
	PendingTxnDeletePullPort = 7780

	// Port to be notified of successful transaction deletes.
	TxnDeletePushPort = 7781

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

var replicaList = flag.String("replicaList", "", "A comma separated list of addresses of Aft replicas")
var gcReplicaList = flag.String("gcReplicaList", "", "A comma separated list of addresses of GC replicas")

func main() {
	flag.Parse()
	if *replicaList == "" {
		fmt.Println("No replicaList provided. Please use the --replicaList flag to pass in a comma-separated list of replicas.")
		os.Exit(1)
	}

	conf := config.ParseConfig()

	var consistencyManager consistency.ConsistencyManager
	switch conf.ConsistencyType {
	case "lww":
		consistencyManager = &consistency.LWWConsistencyManager{}
	case "read-atomic":
		consistencyManager = &consistency.ReadAtomicConsistencyManager{}
	}

	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	replicas := strings.Split(*replicaList, ",")
	numReplicas := len(replicas) - 1
	txnUpdateSockets := make([]*zmq.Socket, numReplicas)
	pendingDeleteSockets := make([]*zmq.Socket, numReplicas)
	deleteSockets := make([]*zmq.Socket, numReplicas)

	for index, replica := range replicas {
		if index < numReplicas { // Skip the last replica because it's an empty string.
			txnUpdateAddress := fmt.Sprintf(PushTemplate, replica, TxnPort)
			socket := createSocket(zmq.PUSH, context, txnUpdateAddress, false)
			txnUpdateSockets[index] = socket

			pendingDeleteAddress := fmt.Sprintf(PushTemplate, replica, PendingTxnDeletePushPort)
			socket = createSocket(zmq.PUSH, context, pendingDeleteAddress, false)
			pendingDeleteSockets[index] = socket

			deleteAddress := fmt.Sprintf(PushTemplate, replica, TxnDeletePushPort)
			socket = createSocket(zmq.PUSH, context, deleteAddress, false)
			deleteSockets[index] = socket
		}
	}

	gcReplicas := strings.Split(*gcReplicaList, ",")
	numGcReplicas := len(gcReplicas) - 1
	gcSockets := make([]*zmq.Socket, numGcReplicas)
	for index, gcReplica := range gcReplicas {
		if index < len(gcReplicas)-1 { // Skip the last replica because it's an empty string.
			gcAddress := fmt.Sprintf(PushTemplate, gcReplica, GcPort)
			socket := createSocket(zmq.PUSH, context, gcAddress, false)
			gcSockets[index] = socket
		}
	}

	txnUpdatePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, TxnPort), true)
	pendingDeletePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, PendingTxnDeletePullPort), true)

	poller := zmq.NewPoller()
	poller.Add(txnUpdatePuller, zmq.POLLIN)
	poller.Add(pendingDeletePuller, zmq.POLLIN)

	allTransactions := map[string]*pb.TransactionRecord{}
	latestVersionIndex := map[string]string{}
	pendingDeleteTransactions := map[string]int{} // TODO: Should this track individual replicas?

	allTransactionsLock := &sync.RWMutex{}
	latestVersionLock := &sync.RWMutex{}
	pendingDeleteTransactionsLock := &sync.RWMutex{}

	go gcRoutine(&allTransactions, allTransactionsLock, &latestVersionIndex,
		latestVersionLock, &pendingDeleteTransactions, pendingDeleteTransactionsLock,
		&pendingDeleteSockets, &consistencyManager,
	)

	reportStart := time.Now()
	currentGcReplica := 0
	deletedTxns := &pb.TransactionList{}

	for true {
		// Wait a 100ms for a new message; we know by default that there is only
		// one socket to poll, so we don't have to check which socket we've
		// received a message on.
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case txnUpdatePuller:
				{
					bts, _ := txnUpdatePuller.RecvBytes(zmq.DONTWAIT)

					txnList := &pb.TransactionList{}
					err = proto.Unmarshal(bts, txnList)
					if err != nil {
						fmt.Println("Unable to parse received TransactionList:\n", err)
						continue
					}

					for _, record := range txnList.Records {
						allTransactionsLock.Lock()
						allTransactions[record.Id] = record
						allTransactionsLock.Unlock()

						for _, key := range record.WriteSet {
							storageKey := consistencyManager.GetStorageKeyName(key, record.Timestamp, record.Id)

							latestVersionLock.Lock()
							latest, ok := latestVersionIndex[key]
							if !ok || consistencyManager.CompareKeys(storageKey, latest) {
								latestVersionIndex[key] = storageKey
							}
							latestVersionLock.Unlock()
						}
					}
				}
			case pendingDeletePuller:
				{
					bts, _ := pendingDeletePuller.RecvBytes(zmq.DONTWAIT)

					txnIdList := &pb.TransactionIdList{}
					err = proto.Unmarshal(bts, txnIdList)
					if err != nil {
						fmt.Println("Unable to parse received TransactionIdList:\n", err)
						continue
					}

					deletedList := &pb.TransactionIdList{}

					for _, id := range txnIdList.Ids {
						// We don't need to check if it's in the map because it is
						// guaranteed to be by the GC process.
						pendingDeleteTransactionsLock.Lock()
						pendingDeleteTransactions[id] += 1
						val := pendingDeleteTransactions[id]
						pendingDeleteTransactionsLock.Unlock()

						if val == numReplicas {
							// Clear the local metadata.
							txn := deleteTransaction(id, &consistencyManager, &allTransactions, allTransactionsLock)

							deletedList.Ids = append(deletedList.Ids, id)
							deletedTxns.Records = append(deletedTxns.Records, txn)
						}
					}

					bts, _ = proto.Marshal(deletedList)
					for _, sckt := range deleteSockets {
						sckt.SendBytes(bts, zmq.DONTWAIT)
					}
				}
			}
		}

		reportEnd := time.Now()

		if reportEnd.Sub(reportStart).Seconds() > 1.0 {
			if len(deletedTxns.Records) > 0 {

				dispatched := 0
				for len(deletedTxns.Records) > 700 {

					// Send 200 transcations per GC replica.
					msg := &pb.TransactionList{Records: deletedTxns.Records[0:200]}
					bts, _ := proto.Marshal(msg)

					socket := gcSockets[currentGcReplica]
					socket.SendBytes(bts, zmq.DONTWAIT)

					deletedTxns.Records = deletedTxns.Records[200:]
					currentGcReplica = (currentGcReplica + 1) % numGcReplicas
					dispatched += 200
				}

				// Send the remaining transactions.
				socket := gcSockets[currentGcReplica]
				bts, _ := proto.Marshal(deletedTxns)
				socket.SendBytes(bts, zmq.DONTWAIT)
				dispatched += len(deletedTxns.Records)

				currentGcReplica = (currentGcReplica + 1) % numGcReplicas
				deletedTxns = &pb.TransactionList{}
			}

			reportStart = time.Now()
		}
	}
}

func deleteTransaction(
	tid string,
	cm *consistency.ConsistencyManager,
	allTransactions *map[string]*pb.TransactionRecord,
	allTransactionsLock *sync.RWMutex,
) *pb.TransactionRecord {
	// Clear the local metadata.
	allTransactionsLock.Lock()
	txn := (*allTransactions)[tid]
	delete(*allTransactions, tid)
	allTransactionsLock.Unlock()

	return txn
}

func gcRoutine(
	allTransactions *map[string]*pb.TransactionRecord,
	allTransactionsLock *sync.RWMutex,
	latestVersionIndex *map[string]string,
	latestVersionLock *sync.RWMutex,
	pendingDeleteTransactions *map[string]int,
	pendingDeleteTransactionsLock *sync.RWMutex,
	pendingDeleteSockets *[]*zmq.Socket,
	cm *consistency.ConsistencyManager,
) {
	for true {
		time.Sleep(1 * time.Millisecond)

		allTransactionsLock.RLock()
		keys := make([]string, len(*allTransactions))
		index := 0
		for tid := range *allTransactions {
			keys[index] = tid
			index += 1
		}
		allTransactionsLock.RUnlock()

		dominatedTransactions := &pb.TransactionIdList{}

		for _, tid := range keys {
			allTransactionsLock.RLock()
			txn := (*allTransactions)[tid]
			allTransactionsLock.RUnlock()

			// We check if txn is nil because we want to make sure that we haven't
			// already deleted this after making it pending.
			if txn != nil && isTransactionDominated(txn, cm, latestVersionIndex, latestVersionLock) {
				dominatedTransactions.Ids = append(dominatedTransactions.Ids, tid)
			}
		}

		if len(dominatedTransactions.Ids) > 0 {
			// Add this TID to the pending delete metadata and notify all replicas.
			for _, tid := range dominatedTransactions.Ids {
				pendingDeleteTransactionsLock.Lock()
				if _, ok := (*pendingDeleteTransactions)[tid]; !ok {
					// Only 0 this out if we have not already marked it as pending
					// to be deleted.
					(*pendingDeleteTransactions)[tid] = 0
				}
				pendingDeleteTransactionsLock.Unlock()
			}

			allTransactionsLock.RLock()
			allTransactionsLock.RUnlock()
			bts, _ := proto.Marshal(dominatedTransactions)
			for _, sckt := range *pendingDeleteSockets {
				sckt.SendBytes(bts, zmq.DONTWAIT)
			}
		}
	}
}

// A transaction is dominated if all the keys in its write set have versions
// that are newer than the version from this transaction.
func isTransactionDominated(
	transaction *pb.TransactionRecord,
	cm *consistency.ConsistencyManager,
	latestVersionIndex *map[string]string,
	latestVersionLock *sync.RWMutex,
) bool {
	for _, key := range transaction.WriteSet {
		if !isKeyVersionDominated(key, transaction, cm, latestVersionIndex, latestVersionLock) {
			return false // If any key version is not dominated, return false.
		}
	}

	// If all key versions are dominated, we return true.
	return true
}

func isKeyVersionDominated(
	key string,
	transaction *pb.TransactionRecord,
	cm *consistency.ConsistencyManager,
	latestVersionIndex *map[string]string,
	latestVersionLock *sync.RWMutex,
) bool {
	// We know the key has to be in the index because we always add it when we
	// hear about a new key.
	storageKey := (*cm).GetStorageKeyName(key, transaction.Timestamp, transaction.Id)

	latestVersionLock.RLock()
	latest, _ := (*latestVersionIndex)[key]
	latestVersionLock.RUnlock()

	return (*cm).CompareKeys(latest, storageKey)
}
