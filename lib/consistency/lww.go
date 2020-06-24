package consistency

import (
	"sync"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type LWWConsistencyManager struct{}

func (lww *LWWConsistencyManager) ValidateTransaction(tid string, readSet map[string]string, writeSet []string) bool {
	return true
}

func (lww *LWWConsistencyManager) GetValidKeyVersion(
	key string,
	transaction *pb.TransactionRecord,
	finishedTransactions *map[string]*pb.TransactionRecord,
	finishedTransactionsLock *sync.RWMutex,
	keyVersionIndex *map[string]*map[string]bool,
	keyVersionIndexLock *sync.RWMutex,
	transactionDependencies *map[string]int,
	transactionDependenciesLock *sync.RWMutex,
	latestVersionIndex *map[string]string,
	latestVersionIndexLock *sync.RWMutex,
) (string, error) {
	return key, nil
}

func (lww *LWWConsistencyManager) GetStorageKeyName(key string, timestamp int64, transactionId string) string {
	return key
}

func (lww *LWWConsistencyManager) CompareKeys(one string, two string) bool {
	return false
}

func (lww *LWWConsistencyManager) UpdateTransactionDependencies(
	keyVersion string,
	finished bool,
	transactionDependencies *map[string]int,
	transactionDependenciesLock *sync.RWMutex,
) {
	return
}
