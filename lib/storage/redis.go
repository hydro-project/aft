package storage

import (
	"context"
	"fmt"
	"os"
	"sync"

	rdslib "github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	pb "github.com/vsreekanti/aft/proto/aft"
)

type RedisStorageManager struct {
	client *rdslib.ClusterClient
}

func NewRedisStorageManager(address string, password string) *RedisStorageManager {
	rc := rdslib.NewClusterClient(&rdslib.ClusterOptions{
		Addrs: []string{address},
	})

	_, err := rc.Ping(context.TODO()).Result()
	if err != nil {
		fmt.Printf("Unexpected error while connecting to Redis client:\n%v\n", err)
		os.Exit(1)
	}

	return &RedisStorageManager{client: rc}
}

func (redis *RedisStorageManager) StartTransaction(id string) error {
	return nil
}

func (redis *RedisStorageManager) CommitTransaction(transaction *pb.TransactionRecord) error {
	key := fmt.Sprintf(TransactionKey, transaction.Id, transaction.Timestamp)
	serialized, err := proto.Marshal(transaction)
	if err != nil {
		return err
	}

	return redis.client.Set(context.TODO(), key, serialized, 0).Err()
}

func (redis *RedisStorageManager) AbortTransaction(transaction *pb.TransactionRecord) error {
	return nil
}

func (redis *RedisStorageManager) Get(key string) (*pb.KeyValuePair, error) {
	result := &pb.KeyValuePair{}

	val, err := redis.client.Get(context.TODO(), key).Result()
	if err != nil {
		return result, err
	}

	err = proto.Unmarshal([]byte(val), result)
	return result, err
}

func (redis *RedisStorageManager) GetTransaction(transactionKey string) (*pb.TransactionRecord, error) {
	result := &pb.TransactionRecord{}

	val, err := redis.client.Get(context.TODO(), transactionKey).Result()
	if err != nil {
		return result, err
	}

	err = proto.Unmarshal([]byte(val), result)
	return result, err
}

func (redis *RedisStorageManager) MultiGetTransaction(transactionKeys *[]string) (*[]*pb.TransactionRecord, error) {
	results := make([]*pb.TransactionRecord, len(*transactionKeys))

	for index, key := range *transactionKeys {
		txn, err := redis.GetTransaction(key)
		if err != nil {
			return &[]*pb.TransactionRecord{}, err
		}

		results[index] = txn
	}

	return &results, nil
}

func (redis *RedisStorageManager) Put(key string, val *pb.KeyValuePair) error {
	serialized, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	return redis.client.Set(context.TODO(), key, serialized, 0).Err()
}

func (redis *RedisStorageManager) MultiPut(data *map[string]*pb.KeyValuePair) error {
	for key, val := range *data {
		err := redis.Put(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (redis *RedisStorageManager) Delete(key string) error {
	return redis.client.Del(context.TODO(), key).Err()
}

func (redis *RedisStorageManager) MultiDelete(keys *[]string) error {
	for _, key := range *keys {
		err := redis.Delete(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (redis *RedisStorageManager) List(prefix string) ([]string, error) {
	result := []string{}
	redisPrefix := fmt.Sprintf("%s*", prefix)
	mtx := &sync.Mutex{}

	err := redis.client.ForEachMaster(context.TODO(), func(_ context.Context, master *rdslib.Client) error {
		cursor := uint64(0)
		additionalKeys := true

		for additionalKeys {
			var scanKeys []string
			var err error
			scanKeys, cursor, err = master.Scan(context.TODO(), cursor, redisPrefix, 100).Result()

			if err != nil {
				return err
			}

			mtx.Lock()
			result = append(result, scanKeys...)
			mtx.Unlock()

			if cursor == 0 {
				additionalKeys = false
			}
		}

		return nil
	})

	if err != nil {
		return []string{}, err
	}

	return result, nil
}
