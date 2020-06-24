package main

import (
	"fmt"
	// "math/rand"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	uuid "github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"

	pb "github.com/vsreekanti/aft/proto/aft"
	annapb "github.com/vsreekanti/aft/proto/anna"
)

type keyUpdate struct {
	key     string
	tid     string
	value   *annapb.LWWValue
	written bool
}

const (
	CommitResponseBase = 9000
	AftQueryPort       = 9100
	AftResponsePort    = 9200
	ContinuePort       = 10000
	DoCommitPort       = 10100
)

func (s *AftServer) StartTransaction(responseAddress string, socketCache *SocketCache) {
	uid, _ := uuid.NewV4()

	tid := uid.String()
	transactionsTs := time.Now().UnixNano()

	txn := &pb.TransactionRecord{
		Id:        tid,
		Timestamp: transactionsTs,
		Status:    pb.TransactionStatus_RUNNING,
		ReplicaId: s.Id,
		WriteSet:  []string{},
		ReadSet:   map[string]string{},
	}

	s.StorageManager.StartTransaction(tid)

	s.RunningTransactionLock.Lock()
	s.RunningTransactions[tid] = txn
	s.RunningTransactionLock.Unlock()

	tag := &pb.TransactionTag{
		Id:      tid,
		Address: s.IpAddress,
		Status:  pb.TransactionStatus_RUNNING,
	}

	bts, _ := proto.Marshal(tag)
	socket := socketCache.Get(responseAddress)
	socket.SendBytes(bts, zmq.DONTWAIT)
	fmt.Printf("Successfully started %s...\n", tid)
}

func (s *AftServer) Continue(bts []byte, context *zmq.Context, socketCache *SocketCache, responseSocket *zmq.Socket, responseAddress string) {
	request := &pb.MetadataRequest{}
	proto.Unmarshal(bts, request)

	address := fmt.Sprintf(PushTemplate, request.Address, ContinuePort)
	socket := socketCache.Get(address)

	message := fmt.Sprintf("%s~%s", request.Tid, responseAddress)
	socket.Send(message, zmq.DONTWAIT)
	bts, _ = responseSocket.RecvBytes(0)

	record := &pb.TransactionRecord{}
	proto.Unmarshal(bts, record)

	s.RunningTransactionLock.Lock()
	s.RunningTransactions[request.Tid] = record
	s.RunningTransactionLock.Unlock()

	s.ContinueAddressCacheLock.Lock()
	s.ContinueAddressCache[request.Tid] = request.Address
	s.ContinueAddressCacheLock.Unlock()

	socket = socketCache.Get(request.ResponseAddress)
	socket.Send("", zmq.DONTWAIT)
	fmt.Printf("Successfully continued %s...\n", request.Tid)
}

func (s *AftServer) doContinue(tid string, responseAddress string, socketCache *SocketCache) {
	s.RunningTransactionLock.RLock()
	record := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	bts, _ := proto.Marshal(record)
	socket := socketCache.Get(responseAddress)
	socket.SendBytes(bts, zmq.DONTWAIT)
}

func (s *AftServer) AftQuery(bts []byte, socketCache *SocketCache) {
	query := &pb.AftQuery{}
	proto.Unmarshal(bts, query)

	response := &pb.AftResponse{Key: query.Key}

	s.UpdateBufferLock.RLock()
	buffer, ok := s.UpdateBuffer[query.Tid]
	s.UpdateBufferLock.RUnlock()

	var returnValue []byte
	found := false
	if ok {
		for _, update := range buffer {
			if update.key == query.Key {
				bts, _ := proto.Marshal(update.value)
				returnValue = bts
				found = true
			}
		}
	}

	if found {
		response.Found = true
		response.Data = returnValue
	} else {
		response.Found = false

		s.ContinueAddressCacheLock.RLock()
		pointer := s.ContinueAddressCache[query.Tid]
		s.ContinueAddressCacheLock.RUnlock()

		response.Pointer = pointer
	}

	socket := socketCache.Get(query.ResponseAddress)
	bts, _ = proto.Marshal(response)
	socket.SendBytes(bts, zmq.DONTWAIT)
}

func (s *AftServer) Write(bts []byte, socketCache *SocketCache) {
	request := &annapb.KeyRequest{}
	proto.Unmarshal(bts, request)

	// This is a hack to get the transaction metadata out of the Cloudburst request.
	tid := ""
	for index, tuple := range request.Tuples {
		if tuple.Key == "TRANSACTION_ID" {
			request.Tuples = append(request.Tuples[:index], request.Tuples[index+1:]...)
			tid = string(tuple.Payload)
			break
		}
	}
	fmt.Printf("Write tid is %s\n", tid)

	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	response := &annapb.KeyResponse{}
	for _, tuple := range request.Tuples {
		key := tuple.Key
		txn.WriteSet = append(txn.WriteSet, key)

		var data *annapb.LWWValue
		if tuple.LatticeType == annapb.LatticeType_LWW {
			lww := &annapb.LWWValue{}
			proto.Unmarshal(tuple.Payload, lww)

			data = lww
		} else {
			fmt.Println("Unexpected and unsupported lattice type: %v.", tuple.LatticeType)
			os.Exit(2)
		}

		s.UpdateBufferLock.Lock()
		s.UpdateBuffer[tid] = append(s.UpdateBuffer[tid],
			&keyUpdate{key: key, tid: tid, value: data})
		s.UpdateBufferLock.Unlock()

		response.Tuples = append(response.Tuples, &annapb.KeyTuple{Key: key})
	}

	socket := socketCache.Get(request.ResponseAddress)
	bts, _ = proto.Marshal(response)

	socket.SendBytes(bts, zmq.DONTWAIT)
}

func (s *AftServer) Read(bts []byte, socketCache *SocketCache, responseSocket *zmq.Socket, responseAddress string) {
	request := &annapb.KeyRequest{}
	proto.Unmarshal(bts, request)

	// This is a hack to get the transaction metadata out of the Cloudburst request.
	tid := ""
	for index, tuple := range request.Tuples {
		if tuple.Key == "TRANSACTION_ID" {
			request.Tuples = append(request.Tuples[:index], request.Tuples[index+1:]...)
			tid = string(tuple.Payload)
			break
		}
	}
	fmt.Printf("Read tid is %s\n", tid)

	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	response := &annapb.KeyResponse{}

	for _, tuple := range request.Tuples {
		var returnValue []byte
		previouslyWritten := false

		// Check if the key is in transaction's write set.
		for _, written := range txn.WriteSet {
			if written == tuple.Key {
				previouslyWritten = true
				break
			}
		}

		// If it was, either return from local or query an upstream cache.
		if previouslyWritten {
			// If the key is in the local update buffer, return it immediately.
			found := false
			s.UpdateBufferLock.RLock()
			buffer, ok := s.UpdateBuffer[txn.Id]
			s.UpdateBufferLock.RUnlock()

			if ok {
				for _, update := range buffer {
					if update.key == tuple.Key {
						bts, _ := proto.Marshal(update.value)
						returnValue = bts
						found = true
					}
				}
			}

			s.ContinueAddressCacheLock.RLock()
			queryAddress := s.ContinueAddressCache[tid]
			s.ContinueAddressCacheLock.RUnlock()

			for !found {
				queryAddress := fmt.Sprintf(PushTemplate, queryAddress, AftQueryPort)
				socket := socketCache.Get(queryAddress)

				query := &pb.AftQuery{
					Key:             tuple.Key,
					Tid:             tid,
					ResponseAddress: responseAddress,
				}

				bts, _ = proto.Marshal(query)
				socket.SendBytes(bts, zmq.DONTWAIT)

				bts, _ = responseSocket.RecvBytes(0)
				response := &pb.AftResponse{}
				proto.Unmarshal(bts, response)

				if response.Found {
					found = true
					returnValue = response.Data
				} else {
					queryAddress = response.Pointer
				}
			}
		} else { // If not, figure out what to return.
			key, err := s.ConsistencyManager.GetValidKeyVersion(
				tuple.Key, txn, &s.FinishedTransactions,
				s.FinishedTransactionLock, &s.KeyVersionIndex,
				s.KeyVersionIndexLock, &s.TransactionDependencies,
				s.TransactionDependenciesLock, &s.LatestVersionIndex,
				s.LatestVersionIndexLock,
			)
			if err != nil {
				break
			}

			// If we've read the key version before, return that version.
			s.ReadCacheLock.RLock()
			val, ok := s.ReadCache[key]
			s.ReadCacheLock.RUnlock()

			if ok {
				returnValue = val.Value
				s.CacheHitCountLock.Lock()
				s.CacheHitCount += 1
				s.CacheHitCountLock.Unlock()
			} else { // Otherwise, get the correct key version from storage.
				kvPair, err := s.StorageManager.Get(key)

				// If the GET request returns an error, that means the key was not
				// accessible, so we return nil.
				if err != nil {
					break
				} else { // Otherwise, add this key to our read cache.
					s.ReadCacheLock.Lock()
					s.ReadCache[key] = *kvPair
					s.ReadCacheLock.Unlock()

					s.KeyVersionIndexLock.Lock()
					(*s.KeyVersionIndex[tuple.Key])[key] = true
					s.KeyVersionIndexLock.Unlock()

					returnValue = kvPair.Value
				}
			}

			if returnValue != nil {
				txn.ReadSet[tuple.Key] = key
			}
		}

		// Hardcode this for now as we don't support cache updates, because there is no
		// Go lattice implementation.
		wrapped := &annapb.LWWValue{Timestamp: 0, Value: returnValue}
		bts, _ := proto.Marshal(wrapped)

		tuple := &annapb.KeyTuple{
			Key:         tuple.Key,
			Payload:     bts,
			LatticeType: annapb.LatticeType_LWW,
		}
		response.Tuples = append(response.Tuples, tuple)
	}

	socket := socketCache.Get(request.ResponseAddress)
	bts, _ = proto.Marshal(response)
	socket.SendBytes(bts, zmq.DONTWAIT)
}

func (s *AftServer) DoCommit(bts []byte, socketCache *SocketCache) {
	commitRequest := &pb.AftCommitRequest{}
	proto.Unmarshal(bts, commitRequest)

	tid := commitRequest.Record.Id
	fmt.Printf("Doing the local commit for %s\n", tid)
	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	err := s.doWrites(commitRequest.Record)

	var result string
	if err != nil {
		fmt.Printf("Writing %v (%s) failed: %v\n", txn.WriteSet, tid, err)
		result = fmt.Sprintf("%v", err)
	} else {
		result = "success"
	}

	socket := socketCache.Get(commitRequest.ResponseAddress)
	socket.Send(result, zmq.DONTWAIT)

	// Move the transaction from the running transactions to the finished set.
	s.RunningTransactionLock.Lock()
	delete(s.RunningTransactions, tid)
	s.RunningTransactionLock.Unlock()

	s.UpdateBufferLock.Lock()
	delete(s.UpdateBuffer, tid)
	s.UpdateBufferLock.Unlock()

	s.ContinueAddressCacheLock.Lock()
	delete(s.ContinueAddressCache, tid)
	s.ContinueAddressCacheLock.Unlock()

	// Update all of the metadata for data this transaction read.
	if txn != nil {
		for _, storageKey := range txn.ReadSet {
			inWriteSet := false

			if !inWriteSet {
				s.ConsistencyManager.UpdateTransactionDependencies(storageKey, true, &s.TransactionDependencies, s.TransactionDependenciesLock)
			}
		}
	}
}

func (s *AftServer) doWrites(record *pb.TransactionRecord) error {
	tid := record.Id

	s.UpdateBufferLock.RLock()
	keyUpdates := make([]*keyUpdate, len(s.UpdateBuffer[record.Id]))
	for index, update := range s.UpdateBuffer[tid] {
		keyUpdates[index] = update
	}
	s.UpdateBufferLock.RUnlock()

	updateData := map[string]*pb.KeyValuePair{}
	for _, update := range keyUpdates {
		key := s.ConsistencyManager.GetStorageKeyName(update.key, record.Timestamp, tid)

		val := &pb.KeyValuePair{
			Key:           update.key,
			Value:         update.value.Value, // Type is LWWValue
			CowrittenKeys: record.WriteSet,
			Tid:           tid,
			Timestamp:     record.Timestamp,
		}

		// NOTE: This line is a hack for getting things to work with Cloudburst
		// because the client interface relies on data not being stored at some
		// modified key but at the exact required key. It would be silly to
		// create a transaction to read a single update, so we're avoiding that
		// for now. This does not violate the consistency constraints since these
		// writes are immutable.
		if len(key) > 3 && !strings.Contains(key, "funcs") {
			updateData[update.key] = val
		} else {
			updateData[key] = val
		}
	}

	return s.StorageManager.MultiPut(&updateData)
}

func (s *AftServer) CommitTransaction(bts []byte, context *zmq.Context, socketCache *SocketCache, responseSocket *zmq.Socket, responseAddress string) {
	commitRequest := &pb.CommitRequest{}
	proto.Unmarshal(bts, commitRequest)

	tid := commitRequest.Tid

	s.RunningTransactionLock.RLock()
	txn, _ := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	if len(txn.WriteSet) > 0 { // Don't actually write anything for this txn if it did no writes.
		aftRequest := &pb.AftCommitRequest{
			Record:          txn,
			ResponseAddress: responseAddress,
		}
		serialized, _ := proto.Marshal(aftRequest)

		for _, address := range commitRequest.Addresses {
			address = fmt.Sprintf(PushTemplate, address, DoCommitPort)
			socket := socketCache.Get(address)
			socket.SendBytes(serialized, zmq.DONTWAIT)
		}

		err := s.doWrites(txn)

		for idx, address := range commitRequest.Addresses {
			response, _ := responseSocket.Recv(0)
			if response != "success" {
				fmt.Printf("\n\n\n *** Commit (%s) failed on response %d from %s: %s. ***\n\n\n", tid, idx, address, response)
				os.Exit(1)
			}
		}

		if err != nil {
			// TODO: Rollback the transaction.
			txn.Status = pb.TransactionStatus_ABORTED
		} else {
			txn.Status = pb.TransactionStatus_COMMITTED
		}

		err = s.StorageManager.CommitTransaction(txn)
		if err != nil {
			fmt.Printf("\n\n\n Commit failed on CommitTransaction (%s): %v\n\n\n", tid, err)
			os.Exit(1)
		}
	} else {
		txn.Status = pb.TransactionStatus_COMMITTED
	}

	socket := socketCache.Get(commitRequest.ResponseAddress)
	response := &pb.TransactionTag{Id: tid, Status: txn.Status}

	bts, _ = proto.Marshal(response)
	socket.SendBytes(bts, zmq.DONTWAIT)

	// Move the transaction from the running transactions to the finished set.
	s.RunningTransactionLock.Lock()
	delete(s.RunningTransactions, tid)
	s.RunningTransactionLock.Unlock()

	s.UpdateBufferLock.Lock()
	delete(s.UpdateBuffer, tid)
	s.UpdateBufferLock.Unlock()

	s.ContinueAddressCacheLock.Lock()
	delete(s.ContinueAddressCache, tid)
	s.ContinueAddressCacheLock.Unlock()

	// Update all of the metadata for data this transaction read.
	for _, storageKey := range txn.ReadSet {
		inWriteSet := false

		if !inWriteSet {
			s.ConsistencyManager.UpdateTransactionDependencies(storageKey, true, &s.TransactionDependencies, s.TransactionDependenciesLock)
		}
	}

	s.UpdateMetadata(&pb.TransactionList{Records: []*pb.TransactionRecord{txn}})
}

func (s *AftServer) AbortTransaction(tid string, responseAddress string, socketCache *SocketCache) {
	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	s.UpdateBufferLock.Lock()
	delete(s.UpdateBuffer, tid)
	s.UpdateBufferLock.Unlock()

	s.StorageManager.AbortTransaction(txn)
	txn.Status = pb.TransactionStatus_ABORTED

	// Move the transaction from the running transactions to the finished set.
	s.RunningTransactionLock.Lock()
	delete(s.RunningTransactions, tid)
	s.RunningTransactionLock.Unlock()

	s.FinishedTransactionLock.Lock()
	s.FinishedTransactions[tid] = txn
	s.FinishedTransactionLock.Unlock()

	s.UpdateMetadata(&pb.TransactionList{Records: []*pb.TransactionRecord{txn}})

	socket := socketCache.Get(responseAddress)
	response := &pb.TransactionTag{Id: tid, Status: txn.Status}

	bts, _ := proto.Marshal(response)
	socket.SendBytes(bts, zmq.DONTWAIT)
}

func (s *AftServer) UpdateMetadata(list *pb.TransactionList) (*empty.Empty, error) {
	for _, record := range list.Records {
		s.FinishedTransactionLock.Lock()
		s.updateKeyVersionIndex(record)

		s.FinishedTransactions[record.Id] = record
		s.FinishedTransactionLock.Unlock()
	}

	return &empty.Empty{}, nil
}

func (s *AftServer) updateKeyVersionIndex(transaction *pb.TransactionRecord) {
	for _, key := range transaction.WriteSet {
		kvName := s.ConsistencyManager.GetStorageKeyName(key, transaction.Timestamp, transaction.Id)

		s.KeyVersionIndexLock.RLock()
		index, ok := s.KeyVersionIndex[key]
		s.KeyVersionIndexLock.RUnlock()

		s.KeyVersionIndexLock.Lock()
		if !ok {
			index = &map[string]bool{}
			(*index)[kvName] = false
			s.KeyVersionIndex[key] = index
		} else {
			(*s.KeyVersionIndex[key])[kvName] = false
		}
		s.KeyVersionIndexLock.Unlock()

		s.LatestVersionIndexLock.Lock()
		latest, ok := s.LatestVersionIndex[key]
		if !ok || s.ConsistencyManager.CompareKeys(kvName, latest) {
			s.LatestVersionIndex[key] = kvName
		}
		s.LatestVersionIndexLock.Unlock()
	}
}

func main() {
	aft, config := NewAftServer()

	seenTransactions := map[string]bool{}
	for seenTxn := range aft.FinishedTransactions {
		seenTransactions[seenTxn] = true
	}

	// Start the multicast goroutine.
	go MulticastRoutine(aft, seenTransactions, config.IpAddress, config.ManagerAddress)

	// Start the local GC routine.
	// go LocalGCRoutine(aft)

	fmt.Printf("Starting server at %s.\n", time.Now().String())

	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unable to start ZMQ context: %v", err)
		os.Exit(1)
	}

	startSocket := createSocket(zmq.PULL, context, "ipc:///requests/start", true)
	commitSocket := createSocket(zmq.PULL, context, "ipc:///requests/commit", true)
	abortSocket := createSocket(zmq.PULL, context, "ipc:///requests/abort", true)
	getSocket := createSocket(zmq.PULL, context, "ipc:///requests/get", true)
	putSocket := createSocket(zmq.PULL, context, "ipc:///requests/put", true)
	updateSocket := createSocket(zmq.PULL, context, "tcp://*:7150", true) // Cache update port.
	continueSocket := createSocket(zmq.PULL, context, "ipc:///requests/continue", true)
	querySocket := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, AftQueryPort), true)

	// A smarter implementation would use something like a socket pool to only
	// keep a handful of these around instead of 10. But sockets are also cheap,
	// so this is not a terrible thing. A pool would just be a cleaner
	// implementation.
	continueResponseSockets := make([]*zmq.Socket, 10)
	continueResponseAddresses := make([]string, 10)
	continueResponseIndex := 0
	for i := 0; i < 10; i++ {
		socket := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, CommitResponseBase+i), true)
		response := fmt.Sprintf(PushTemplate, aft.IpAddress, CommitResponseBase+i)

		continueResponseSockets[i] = socket
		continueResponseAddresses[i] = response
	}

	queryResponseSockets := make([]*zmq.Socket, 10)
	queryResponseAddresses := make([]string, 10)
	queryResponseIndex := 0
	for i := 0; i < 10; i++ {
		socket := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, AftResponsePort+i), true)
		response := fmt.Sprintf(PushTemplate, aft.IpAddress, AftResponsePort+i)

		queryResponseSockets[i] = socket
		queryResponseAddresses[i] = response
	}

	doContinueSocket := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, ContinuePort), true)
	doCommitSocket := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, DoCommitPort), true)

	poller := zmq.NewPoller()
	poller.Add(startSocket, zmq.POLLIN)
	poller.Add(commitSocket, zmq.POLLIN)
	poller.Add(abortSocket, zmq.POLLIN)
	poller.Add(getSocket, zmq.POLLIN)
	poller.Add(putSocket, zmq.POLLIN)
	poller.Add(updateSocket, zmq.POLLIN)
	poller.Add(continueSocket, zmq.POLLIN)
	poller.Add(querySocket, zmq.POLLIN)
	poller.Add(doContinueSocket, zmq.POLLIN)
	poller.Add(doCommitSocket, zmq.POLLIN)

	socketCache := NewSocketCache(context)

	for true {
		// Wait 10ms for a new message.
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {

			case startSocket:
				{
					responseAddress, _ := s.Recv(zmq.DONTWAIT)
					go aft.StartTransaction(responseAddress, socketCache)
				}
			case commitSocket:
				{
					bts, _ := s.RecvBytes(zmq.DONTWAIT)

					socket := continueResponseSockets[continueResponseIndex]
					go aft.CommitTransaction(bts, context, socketCache, socket, continueResponseAddresses[continueResponseIndex])
					continueResponseIndex = (continueResponseIndex + 1) % 10
				}
			case abortSocket:
				{
					message, _ := s.Recv(zmq.DONTWAIT)
					splits := strings.Split(message, "~")

					go aft.AbortTransaction(splits[0], splits[1], socketCache)
				}
			case getSocket:
				{
					bts, _ := s.RecvBytes(zmq.DONTWAIT)

					socket := queryResponseSockets[queryResponseIndex]
					go aft.Read(bts, socketCache, socket, queryResponseAddresses[queryResponseIndex])
					queryResponseIndex = (queryResponseIndex + 1) % 10
				}
			case putSocket:
				{
					bts, _ := s.RecvBytes(zmq.DONTWAIT)

					go aft.Write(bts, socketCache)
				}
			case updateSocket:
				{
					// We don't currently support receivingKVS updates in the Go/AFT cache because
					// we don't have Go lattices implemented.
					continue
				}
			case continueSocket:
				{
					bts, _ := s.RecvBytes(zmq.DONTWAIT)

					socket := continueResponseSockets[continueResponseIndex]
					go aft.Continue(bts, context, socketCache, socket, continueResponseAddresses[continueResponseIndex])
					continueResponseIndex = (continueResponseIndex + 1) % 10
				}
			case querySocket:
				{
					bts, _ := s.RecvBytes(zmq.DONTWAIT)

					go aft.AftQuery(bts, socketCache)
				}
			case doContinueSocket:
				{
					message, _ := s.Recv(zmq.DONTWAIT)
					splits := strings.Split(message, "~")

					go aft.doContinue(splits[0], splits[1], socketCache)
				}
			case doCommitSocket:
				{
					bts, _ := s.RecvBytes(zmq.DONTWAIT)

					go aft.DoCommit(bts, socketCache)
				}
			}
		}

		// NOTE: If someone wanted to make this a full-fledged cache implementation
		// for Hydro, we'd first need to implement lattices in Go, then clear up
		// the weirdness with having two different Protobuf formats for the AFT
		// metadata vs. the Anna metadata*, then add a replica of the C++ cache
		// propagation logic here. Note that it's not clear this would even work
		// well with the Cloudburst scheduling heuristics because of AFT.

		// *A simple solution to this mismatch might be to receive a lattice from
		// the Python side, unwrap it, dump the data into an AFT metadata packate,
		// then rewrap it with the same lattice metadata, and do the opposite on
		// each read. It's likely there's a more efficient way to do this though...
	}
}
