package consistency

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type ReadAtomicConsistencyManager struct{}

const (
	keyTemplate = "data/%s/%d/%s"
)

func (racm *ReadAtomicConsistencyManager) ValidateTransaction(tid string, readSet map[string]string, writeSet []string) bool {
	return true
}

type KeyVersionMetadata struct {
	id        string
	timestamp int64
	version   string
	cached    bool
}

func (racm *ReadAtomicConsistencyManager) GetValidKeyVersion(
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

	if val, ok := transaction.ReadSet[key]; ok {
		return val, nil
	}

	latestVersionIndexLock.RLock()
	latestVersion, ok := (*latestVersionIndex)[key]
	latestVersionIndexLock.RUnlock()

	// Check to see if the most recent version is valid, and if so return it
	// immediately.
	if ok {
		fmt.Println(latestVersion)
		fmt.Println(splitKey(latestVersion))
		_, latestTxnId := splitKey(latestVersion)
		finishedTransactionsLock.RLock()
		latestTxn := (*finishedTransactions)[latestTxnId]
		finishedTransactionsLock.RUnlock()
		isLatestValid := true

		// Check to see if the latest version of the key we know about is valid or
		// not.
		for _, latestCowritten := range latestTxn.WriteSet {
			for coread := range transaction.ReadSet {
				if latestCowritten == coread {
					coreadVersion := transaction.ReadSet[coread]
					cowrittenVersion := racm.GetStorageKeyName(latestCowritten, latestTxn.Timestamp, latestTxn.Id)

					if racm.CompareKeys(cowrittenVersion, coreadVersion) { // If the latest version contains a write we should've read we ignore it.
						isLatestValid = false
						break
					}
				}
			}

			if !isLatestValid {
				break
			}
		}

		if isLatestValid {
			racm.UpdateTransactionDependencies(latestVersion, false, transactionDependencies, transactionDependenciesLock)
			return latestVersion, nil
		}
	}

	// Check if the key version is constrained by any of the keys we've already
	// read.
	constraintSet := []string{}
	for _, fullName := range transaction.ReadSet {
		_, tid := splitKey(fullName)
		// We ignore the boolean because we are guaranteed to have the key in the
		// readCache.
		finishedTransactionsLock.RLock()
		txn, ok := (*finishedTransactions)[tid]
		finishedTransactionsLock.RUnlock()
		if !ok {
			return "", errors.New(fmt.Sprintf("Unexpected metadata loss."))
		}

		for _, cowritten := range txn.WriteSet {
			if key == cowritten {
				constraintSet = append(constraintSet, racm.GetStorageKeyName(key, txn.Timestamp, txn.Id))
			}
		}
	}

	// Pick the latest timestamp in the constraint set if there are any keys in
	// it.
	constraintEarliest := ""
	if len(constraintSet) > 0 {
		constraintEarliest := constraintSet[0]

		for _, constraint := range constraintSet {
			if racm.CompareKeys(constraint, constraintEarliest) {
				constraintEarliest = constraint
			}
		}
	}

	// Retrieve all of the versions available for this key.
	keyVersionIndexLock.RLock()
	kvList, ok := (*keyVersionIndex)[key]

	if !ok || len(*kvList) == 0 {
		if len(constraintEarliest) == 0 { // We don't know about any other keys, so if there were no constraints, we return an error.
			keyVersionIndexLock.RUnlock()
			return "", errors.New(fmt.Sprintf("There are no versions of key %s.", key))
		} else { // Otherwise, we return the constraint.
			racm.UpdateTransactionDependencies(constraintEarliest, false, transactionDependencies, transactionDependenciesLock)
			return constraintEarliest, nil
		}
	}

	versionMetadata := make([]KeyVersionMetadata, len(*kvList))
	index := 0

	// Construct the set of keys that are newer than the constraint earliest (or
	// all keys if no constraints are present) that we will iterate over.
	for kv := range *kvList {
		if constraintEarliest == "" || racm.CompareKeys(constraintEarliest, kv) {
			tts, tid := splitKey(kv)
			versionMetadata[index] = KeyVersionMetadata{id: tid, timestamp: tts, version: kv, cached: (*kvList)[kv]}
			index += 1

			if index > 5000 {
				break // We only consider 5000 key versions.
			}
		}
	}
	keyVersionIndexLock.RUnlock()

	// Sort versionMetadata in reverse order.
	sort.Slice(versionMetadata, func(i int, j int) bool {
		return versionMetadata[i].timestamp > versionMetadata[j].timestamp ||
			(versionMetadata[i].timestamp == versionMetadata[j].timestamp &&
				versionMetadata[i].id > versionMetadata[j].id)
	})

	latest := ""
	isCached := false

	for _, keyVersion := range versionMetadata {
		validVersion := true

		finishedTransactionsLock.RLock()
		keyTxn := (*finishedTransactions)[keyVersion.id]
		finishedTransactionsLock.RUnlock()

		// Check to see if this version has any write conflicts with what we've
		// already written.
		for read := range transaction.ReadSet {
			readVersion := racm.GetStorageKeyName(read, transaction.Timestamp, transaction.Id)
			if len(keyVersion.version) == 0 { // This key was already deleted.
				validVersion = false
				break
			}

			if !racm.CompareKeys(readVersion, keyVersion.version) {
				for _, cowritten := range keyTxn.WriteSet {
					if cowritten == readVersion {
						validVersion = false
						break
					}
				}

				if !validVersion {
					break
				}
			}
		}

		// If we've seen nothing valid before, just pick the first valid thing
		// we've seen. Alternately, if we find something slightly older that is
		// cached, we use that instead.
		if validVersion && (len(latest) == 0 || (keyVersion.cached && !isCached)) {
			latest = keyVersion.version
			isCached = keyVersion.cached
		}

		// If we've found a cached key or looked at more than 10 versions and
		// haven't found a cached one, we just return immediately, as long as
		// something is valid.
		if isCached || ((index >= 10 && len(latest) > 0) && len(constraintEarliest) > 0) {
			break
		}
	}

	if len(latest) == 0 {
		if len(constraintEarliest) == 0 { // If we found no valid keys and no constraints, we return an error.
			return "", errors.New(fmt.Sprintf("There are no valid versions of key %s.", key))
		} else { // If we just found no valid keys newer than the constraint, we just return the constraint.
			latest = constraintEarliest
		}
	}

	racm.UpdateTransactionDependencies(latest, false, transactionDependencies, transactionDependenciesLock)
	return latest, nil
}

func (racm *ReadAtomicConsistencyManager) UpdateTransactionDependencies(
	keyVersion string,
	finished bool,
	transactionDependencies *map[string]int,
	transactionDependenciesLock *sync.RWMutex,
) {
	_, tid := splitKey(keyVersion)

	transactionDependenciesLock.Lock()
	if !finished {
		if _, ok := (*transactionDependencies)[tid]; !ok {
			(*transactionDependencies)[tid] = 0
		}

		(*transactionDependencies)[tid] += 1
	} else {
		// We don't need to check if the key exists because we're guaranteed to have created this data.
		(*transactionDependencies)[tid] -= 1
	}

	transactionDependenciesLock.Unlock()
}

func (racm *ReadAtomicConsistencyManager) GetStorageKeyName(key string, timestamp int64, transactionId string) string {
	return fmt.Sprintf(keyTemplate, key, timestamp, transactionId)
}

// This function takes in two keys that are expected to conform to the string
// format defined in keyTemplate above. It returns true if the key passed in as
// argument `one` is newer than the key passed in as argument `two`. It returns
// false otherwise.
func (racm *ReadAtomicConsistencyManager) CompareKeys(one string, two string) bool {
	oneTs, oneTid := splitKey(one)
	twoTs, twoTid := splitKey(two)

	return oneTs > twoTs || (oneTs == twoTs && oneTid > twoTid)
}

// This function takes in a single key that conforms to the keyTemplate defined
// above, and it returns the timestamp and UUID of the transaction that wrote
// it.
func splitKey(key string) (int64, string) {
	splits := strings.Split(key, "/")

	// We know err won't be nil unless someone is interfering with the system.
	txnTs, _ := strconv.ParseInt(splits[2], 10, 64)
	tid := splits[3]

	return txnTs, tid
}
