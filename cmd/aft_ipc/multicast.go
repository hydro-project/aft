package main

import (
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"

	pb "github.com/vsreekanti/aft/proto/aft"
	annapb "github.com/vsreekanti/aft/proto/anna"
)

const (
	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"

	// Ports to notify and be notified of new transactions.
	TxnPort = 7777

	// Ports to notify and be notified of pending transaction deletes.
	PendingTxnDeletePullPort = 7779
	PendingTxnDeletePushPort = 7780

	// Port to be notified of successful transaction deletes.
	TxnDeletePullPort = 7781
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
		fmt.Println("address is", address)
		fmt.Println("Unexpected error while binding/connecting socket:\n", err)
		os.Exit(1)
	}

	return sckt
}

func updateCaches(replicaListRequestor *zmq.Socket) *[]string {
	replicaListRequestor.Send("", zmq.DONTWAIT) // The contents of the message don't matter.
	bts, _ := replicaListRequestor.RecvBytes(0)

	result := &annapb.StringSet{}
	proto.Unmarshal(bts, result)

	return &result.Keys
}

func MulticastRoutine(
	server *AftServer,
	seenTransactions map[string]bool,
	ipAddress string,
	managerAddress string) {
	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	// Sockets to receive and send notifications about new transactions.
	updatePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, TxnPort), true)
	managerPusher := createSocket(zmq.PUSH, context, fmt.Sprintf(PushTemplate, managerAddress, TxnPort), false)

	replicaListRequestor := createSocket(zmq.REQ, context, fmt.Sprintf(PushTemplate, managerAddress, 7007), false)
	replicaList := updateCaches(replicaListRequestor)

	updatePushers := map[string]*zmq.Socket{}
	for _, replica := range *replicaList {
		if replica != ipAddress {
			address := fmt.Sprintf(PushTemplate, replica, TxnPort)
			pusher := createSocket(zmq.PUSH, context, address, false)
			updatePushers[replica] = pusher
		}
	}

	// Sockets to receive and send notifications about pending transaction
	// deletes.
	pendingDeletePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, PendingTxnDeletePullPort), true)
	pendingDeletePusher := createSocket(zmq.PUSH, context, fmt.Sprintf(PushTemplate, managerAddress, PendingTxnDeletePushPort), false)

	// Sockets to pull udpates about successfully deleted transactions.
	deletePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, TxnDeletePullPort), true)

	// Create a new poller to wait for new updates.
	poller := zmq.NewPoller()
	poller.Add(updatePuller, zmq.POLLIN)
	poller.Add(pendingDeletePuller, zmq.POLLIN)
	poller.Add(deletePuller, zmq.POLLIN)

	// We use this map to make sure we don't say to GC the same transaction more
	// than once.
	deletedMarkedTransactions := map[string]bool{}

	reportStart := time.Now()
	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case updatePuller:
				{
					bts, _ := updatePuller.RecvBytes(zmq.DONTWAIT)

					newTransactions := &pb.TransactionList{}
					err = proto.Unmarshal(bts, newTransactions)
					if err != nil {
						fmt.Println("Unexpected error while deserializing Protobufs:\n", err)
						continue
					}
					fmt.Printf("Received %d new transactions.\n", len(newTransactions.Records))

					// Filter out any transactions we have already heard about.
					unseenTransactions := &pb.TransactionList{}
					for _, record := range newTransactions.Records {
						if _, ok := seenTransactions[record.Id]; !ok {
							seenTransactions[record.Id] = true

							if !isTransactionDominated(record, server) {
								unseenTransactions.Records = append(unseenTransactions.Records, record)
							}
						}
					}

					server.UpdateMetadata(unseenTransactions)
				}
			case pendingDeletePuller:
				{
					bts, _ := pendingDeletePuller.RecvBytes(zmq.DONTWAIT)

					pendingDeleteIds := &pb.TransactionIdList{}
					err = proto.Unmarshal(bts, pendingDeleteIds)
					if err != nil {
						fmt.Println("Unexpected error while deserializing Protobufs:\n", err)
						continue
					}

					validDeleteIds := &pb.TransactionIdList{}
					for _, id := range pendingDeleteIds.Ids {
						// Make sure we don't delete a transaction we haven't seen before.
						if _, ok := seenTransactions[id]; ok {
							// Only delete it if we've already considered it dominated
							// locally and we have no dependencies on it. We are guaranteed
							// to have no dependencies because we already deleted it.
							server.LocallyDeletedTransactionsLock.RLock()
							_, deleted := server.LocallyDeletedTransactions[id]
							_, marked := deletedMarkedTransactions[id]
							server.LocallyDeletedTransactionsLock.RUnlock()

							if deleted && !marked {
								validDeleteIds.Ids = append(validDeleteIds.Ids, id)
								deletedMarkedTransactions[id] = true
							}
						} else {
							// If we haven't seen it before, we know it'll be dominated by
							// what we receive, so we mark it locally and just ignore it if
							// it ever arrives.
							seenTransactions[id] = true
							deletedMarkedTransactions[id] = true

							validDeleteIds.Ids = append(validDeleteIds.Ids, id)
						}
					}

					if len(validDeleteIds.Ids) > 0 {
						bts, _ = proto.Marshal(validDeleteIds)
						pendingDeletePusher.SendBytes(bts, zmq.DONTWAIT)
					}
				}
			case deletePuller:
				{
					bts, _ := deletePuller.RecvBytes(zmq.DONTWAIT)

					deleteIds := &pb.TransactionIdList{}
					err = proto.Unmarshal(bts, deleteIds)
					if err != nil {
						fmt.Println("Unexpected error while deserializing Protobufs:\n", err)
						continue
					}

					// Delete the successfully deleted IDs from our local seen
					// transactions metadata.
					for _, id := range deleteIds.Ids {
						delete(seenTransactions, id)
						delete(deletedMarkedTransactions, id)

						server.FinishedTransactionLock.Lock()
						delete(server.FinishedTransactions, id)
						server.FinishedTransactionLock.Unlock()

						server.LocallyDeletedTransactionsLock.Lock()
						delete(server.LocallyDeletedTransactions, id)
						server.LocallyDeletedTransactionsLock.Unlock()
					}
				}
			}
		}

		reportEnd := time.Now()
		if reportEnd.Sub(reportStart).Seconds() > 1.0 {
			replicaList = updateCaches(replicaListRequestor)

			for _, replica := range *replicaList {
				if _, ok := updatePushers[replica]; ok {
					if replica != ipAddress {
						address := fmt.Sprintf(PushTemplate, replica, TxnPort)
						pusher := createSocket(zmq.PUSH, context, address, false)
						updatePushers[replica] = pusher
					}
				}
			}

			// Lock the mutex, serialize the newly committed transactions, and unlock.
			server.FinishedTransactionLock.RLock()
			replicaMessage := pb.TransactionList{}
			gcMessage := pb.TransactionList{}

			for tid := range server.FinishedTransactions {
				if _, ok := seenTransactions[tid]; !ok {
					record := server.FinishedTransactions[tid]

					server.LocallyDeletedTransactionsLock.RLock()
					_, ok := server.LocallyDeletedTransactions[tid]
					server.LocallyDeletedTransactionsLock.RUnlock()

					// Only send this message to other replicas if it's not already dominated.
					if !ok {
						replicaMessage.Records = append(replicaMessage.Records, record)
					}

					gcMessage.Records = append(gcMessage.Records, record)
					seenTransactions[tid] = true
				}
			}
			server.FinishedTransactionLock.RUnlock()

			if len(replicaMessage.Records) > 0 {
				bts, err := proto.Marshal(&replicaMessage)
				if err != nil {
					fmt.Println("Unexpected error while marshaling Protobufs:\n", err)
					continue
				}

				for _, pusher := range updatePushers {
					pusher.SendBytes(bts, zmq.DONTWAIT)
				}
			}

			if len(gcMessage.Records) > 0 {
				bts, err := proto.Marshal(&gcMessage)
				if err != nil {
					fmt.Println("Unexpected error while marshaling Protobufs:\n", err)
					continue
				}

				managerPusher.SendBytes(bts, zmq.DONTWAIT)
			}

			reportStart = time.Now()
		}
	}
}

func LocalGCRoutine(server *AftServer) {
	for true {
		dominatedTransactions := map[string]*pb.TransactionRecord{}

		// We create a local copy of the keys so that we don't modify the list
		// while we are iterating over it. Otherwise, we'd have to RLock for the
		// whole GC duration.
		server.FinishedTransactionLock.RLock()
		finished := map[string]*pb.TransactionRecord{}
		for tid := range server.FinishedTransactions {
			finished[tid] = server.FinishedTransactions[tid]
		}
		server.FinishedTransactionLock.RUnlock()

		for tid, txn := range finished {
			if txn != nil && isTransactionDominated(txn, server) {
				server.LocallyDeletedTransactionsLock.RLock()
				if _, ok := server.LocallyDeletedTransactions[tid]; !ok {
					// We can skip this record if we've already looked at this txn.
					server.TransactionDependenciesLock.RLock()
					if val, ok := server.TransactionDependencies[tid]; !ok || val == 0 {
						dominatedTransactions[tid] = txn
					}
					server.TransactionDependenciesLock.RUnlock()
				}
				server.LocallyDeletedTransactionsLock.RUnlock()
			}

			if len(dominatedTransactions) > 1000 {
				break
			}
		}

		if len(dominatedTransactions) > 0 {
			go transactionClearRoutine(dominatedTransactions, server)
		}
	}
}

func transactionClearRoutine(dominatedTransactions map[string]*pb.TransactionRecord, server *AftServer) {
	// Delete all of the key version index data, then clean up committed
	// transactions and the read cache.
	cachedKeys := []string{}
	for dominatedTid := range dominatedTransactions {
		txn := dominatedTransactions[dominatedTid]
		for _, key := range txn.WriteSet {
			keyVersion := server.ConsistencyManager.GetStorageKeyName(key, txn.Timestamp, txn.Id)

			server.KeyVersionIndexLock.Lock()
			if (*server.KeyVersionIndex[key])[keyVersion] {
				cachedKeys = append(cachedKeys, keyVersion)
			}

			delete(*server.KeyVersionIndex[key], keyVersion)
			server.KeyVersionIndexLock.Unlock()
		}
	}

	for tid := range dominatedTransactions {
		server.LocallyDeletedTransactionsLock.Lock()
		server.LocallyDeletedTransactions[tid] = true
		server.LocallyDeletedTransactionsLock.Unlock()

		// server.FinishedTransactionLock.Lock()
		// delete(server.FinishedTransactions, tid)
		// server.FinishedTransactionLock.Unlock()

		server.TransactionDependenciesLock.Lock()
		delete(server.TransactionDependencies, tid)
		server.TransactionDependenciesLock.Unlock()
	}

	// Consider moving these locks inside the loop.
	for _, keyVersion := range cachedKeys {
		server.ReadCacheLock.Lock()
		delete(server.ReadCache, keyVersion)
		server.ReadCacheLock.Unlock()
	}
}

// A transaction is dominated if all the keys in its write set have versions
// that are newer than the version from this transaction.
func isTransactionDominated(transaction *pb.TransactionRecord, server *AftServer) bool {
	dominated := true
	for _, key := range transaction.WriteSet {
		if !isKeyVersionDominated(key, transaction, server) {
			dominated = false
		}
	}

	return dominated
}

func isKeyVersionDominated(key string, transaction *pb.TransactionRecord, server *AftServer) bool {
	// We might not have heard of this key; if so, it can't be determined to be
	// dominated.
	server.LatestVersionIndexLock.RLock()
	latest, ok := server.LatestVersionIndex[key]
	server.LatestVersionIndexLock.RUnlock()

	if !ok {
		return false
	}

	storageKey := server.ConsistencyManager.GetStorageKeyName(key, transaction.Timestamp, transaction.Id)
	return server.ConsistencyManager.CompareKeys(latest, storageKey)
}
