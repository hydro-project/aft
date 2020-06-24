package storage

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type S3StorageManager struct {
	bucket   string
	s3Client *awss3.S3
}

func NewS3StorageManager(bucket string) *S3StorageManager {
	s3c := awss3.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	return &S3StorageManager{bucket: bucket, s3Client: s3c}
}

func (s3 *S3StorageManager) StartTransaction(id string) error {
	return nil
}

func (s3 *S3StorageManager) CommitTransaction(transaction *pb.TransactionRecord) error {
	key := fmt.Sprintf(TransactionKey, transaction.Id, transaction.Timestamp)
	serialized, err := proto.Marshal(transaction)
	if err != nil {
		return err
	}

	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(serialized),
	}

	_, err = s3.s3Client.PutObject(input)

	return err
}

func (s3 *S3StorageManager) AbortTransaction(transaction *pb.TransactionRecord) error {
	return nil
}

func (s3 *S3StorageManager) Get(key string) (*pb.KeyValuePair, error) {
	input := &awss3.GetObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	result := &pb.KeyValuePair{}

	getObjectOutput, err := s3.s3Client.GetObject(input)
	if err != nil {
		return result, err
	}

	body := new(bytes.Buffer)
	_, err = body.ReadFrom(getObjectOutput.Body)
	err = proto.Unmarshal(body.Bytes(), result)

	return result, err
}

func (s3 *S3StorageManager) GetTransaction(transactionKey string) (*pb.TransactionRecord, error) {
	input := &awss3.GetObjectInput{
		Bucket: &s3.bucket,
		Key:    &transactionKey,
	}

	result := &pb.TransactionRecord{}

	getObjectOutput, err := s3.s3Client.GetObject(input)
	if err != nil {
		return result, err
	}

	body := new(bytes.Buffer)
	_, err = body.ReadFrom(getObjectOutput.Body)
	err = proto.Unmarshal(body.Bytes(), result)

	return result, err
}

func (s3 *S3StorageManager) MultiGetTransaction(transactionKeys *[]string) (*[]*pb.TransactionRecord, error) {
	results := make([]*pb.TransactionRecord, len(*transactionKeys))

	for index, key := range *transactionKeys {
		txn, err := s3.GetTransaction(key)
		if err != nil {
			return &[]*pb.TransactionRecord{}, err
		}

		results[index] = txn
	}

	return &results, nil
}

func (s3 *S3StorageManager) Put(key string, val *pb.KeyValuePair) error {
	serialized, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(serialized),
	}

	_, err = s3.s3Client.PutObject(input)

	return err
}

func (s3 *S3StorageManager) MultiPut(data *map[string]*pb.KeyValuePair) error {
	for key, val := range *data {
		err := s3.Put(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s3 *S3StorageManager) Delete(key string) error {
	input := &awss3.DeleteObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	_, err := s3.s3Client.DeleteObject(input)
	return err
}

func (s3 *S3StorageManager) MultiDelete(keys *[]string) error {
	for _, key := range *keys {
		err := s3.Delete(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s3 *S3StorageManager) List(prefix string) ([]string, error) {
	input := &awss3.ListObjectsV2Input{
		Bucket: &s3.bucket,
		Prefix: &prefix,
	}

	additionalKeys := true
	returnValue := []string{}

	for additionalKeys {
		result, err := s3.s3Client.ListObjectsV2(input)
		if err != nil {
			return nil, err
		}

		for _, val := range result.Contents {
			returnValue = append(returnValue, *val.Key)
		}

		if *result.IsTruncated {
			input.ContinuationToken = result.NextContinuationToken
		} else {
			additionalKeys = false
		}
	}

	return returnValue, nil
}
