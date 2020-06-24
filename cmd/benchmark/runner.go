package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/montanaflynn/stats"
	"google.golang.org/grpc"

	pb "github.com/vsreekanti/aft/proto/aft"
)

var numRequests = flag.Int("numRequests", 1000, "The total number of requests in the benchmark")
var numThreads = flag.Int("numThreads", 10, "The total number of parallel threads in the benchmark")
var numKeys = flag.Int64("numKeys", 1000, "The number of keys to operate over")
var numReads = flag.Int64("numReads", 2, "The number of reads to do per transaction")
var replicaList = flag.String("replicaList", "", "A comma separated list of addresses of Aft replicas")
var address = flag.String("address", "", "The Aft ELB address to communicate with")
var benchmarkType = flag.String("benchmarkType", "", "The type of benchmark to run. Options are aft, plain, and local.")
var length = flag.Int("length", 2, "The number of functions to execute per request.")

func main() {
	// Parse command line flags.
	flag.Parse()

	// Setup the logging infrastructure.
	f, err := os.OpenFile("log.txt", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Println("Could not open log file:\n", err)
		os.Exit(1)
	}
	log.SetOutput(f)

	if *benchmarkType == "" {
		fmt.Println("No benchmarkType provided. Defaulting to aft...")
		*benchmarkType = "aft"
	}

	if (*benchmarkType == "aft" || *benchmarkType == "local") && *address == "" {
		fmt.Printf("No Aft service address provided for benchmark of type %s. Please use the --address flag.\n", *benchmarkType)
		os.Exit(1)
	}

	replicas := strings.Split(*replicaList, ",")

	requestsPerThread := int64(*numRequests / *numThreads)
	fmt.Printf("Starting benchmark type %s with %d requests across %d threads...\n", *benchmarkType, *numRequests, *numThreads)

	latencyChannel := make(chan []float64)
	errorChannel := make(chan []string)
	totalTimeChannel := make(chan float64)

	for tid := 0; tid < *numThreads; tid++ {
		switch *benchmarkType {
		case "aft":
			go benchmark(tid, requestsPerThread, *length, *address, latencyChannel, errorChannel, totalTimeChannel)
		case "plain":
			go benchmarkPlain(tid, requestsPerThread, latencyChannel, errorChannel, totalTimeChannel)
		case "local":
			go benchmarkLocal(tid, requestsPerThread, replicas, latencyChannel, errorChannel, totalTimeChannel)
		}
	}

	latencies := []float64{}
	errors := []string{}
	thruputs := []float64{}

	for tid := 0; tid < *numThreads; tid++ {
		latencyArray := <-latencyChannel
		latencies = append(latencies, latencyArray...)

		errorArray := <-errorChannel
		errors = append(errors, errorArray...)

		threadTime := <-totalTimeChannel
		threadThruput := float64(requestsPerThread) / threadTime
		thruputs = append(thruputs, threadThruput)
	}

	median, _ := stats.Median(latencies)
	fifth, _ := stats.Percentile(latencies, 5.0)
	nfifth, _ := stats.Percentile(latencies, 95.0)
	first, _ := stats.Percentile(latencies, 1.0)
	nninth, _ := stats.Percentile(latencies, 99.0)
	totalThruput, _ := stats.Sum(thruputs)

	if len(errors) > 0 {
		fmt.Printf("Errors: %v\n", errors)
	}

	fmt.Printf("Number of errors: %d\n", len(errors))
	fmt.Printf("Median latency: %.6f\n", median)
	fmt.Printf("5th percentile/95th percentile: %.6f, %.6f\n", fifth, nfifth)
	fmt.Printf("1st percentile/99th percentile: %.6f, %.6f\n", first, nninth)
	fmt.Printf("Total throughput: %f\n", totalThruput)
}

// Runs a benchmark by invoking the aft-test Lambda function, which uses Aft
// for read atomic consistency.
func benchmark(
	tid int,
	threadRequestCount int64,
	requestLength int,
	address string,
	latencyChannel chan []float64,
	errorChannel chan []string,
	totalTimeChannel chan float64,
) {
	errors := []string{}
	latencies := []float64{}

	lambdaClient := lambda.New(
		session.New(),
		&aws.Config{Region: aws.String(endpoints.UsEast1RegionID)},
	)

	type lambdaInput struct {
		Count   int    `json:"count"`
		Reads   int    `json:"reads"`
		Address string `json:"address"`
		Length  int    `json:"length"`
	}

	pyld := lambdaInput{
		Count:   1,
		Address: address,
		Reads:   int(*numReads),
		Length:  requestLength,
	}
	payload, _ := json.Marshal(&pyld)

	input := &lambda.InvokeInput{
		FunctionName:   aws.String("aft-test"),
		Payload:        payload,
		InvocationType: aws.String("RequestResponse"),
	}

	benchStart := time.Now()
	epochStart := time.Now()
	epochId := 0
	epochRequests := 0
	epochLength := 2.0

	requestId := int64(0)
	for ; requestId < threadRequestCount; requestId++ {

		epochEnd := time.Now()
		if epochEnd.Sub(epochStart).Seconds() > epochLength {
			log.Println(fmt.Sprintf("%s: Epoch %d: %f", time.Now().String(), epochId, float64(epochRequests)/epochLength))

			epochId += 1
			epochRequests = 0
			epochStart = time.Now()
		}

		requestStart := time.Now()
		response, err := lambdaClient.Invoke(input)
		requestEnd := time.Now()

		// Log the elapsed request time.
		latencies = append(latencies, requestEnd.Sub(requestStart).Seconds())

		// First, we check if the request itself returned an error. This should be
		// very unlikely.
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			// Next, we try to parse the response.
			bts := string(response.Payload)

			if !strings.Contains(bts, "Success") {
				errors = append(errors, bts)
			} else {
				epochRequests += 1
			}
		}
	}

	benchEnd := time.Now()
	totalTime := benchEnd.Sub(benchStart).Seconds()

	latencyChannel <- latencies
	errorChannel <- errors
	totalTimeChannel <- totalTime
}

// A benchmark that calls either the ddb-txn or the no-aft functions. The
// ddb-txn Lambda invokes DynamoDB in transaction mode, and the no-aft function
// talks directly to the underlying storage system. Both functions report back
// inconsistencies observed.
func benchmarkPlain(
	tid int,
	threadRequestCount int64,
	latencyChannel chan []float64,
	errorChannel chan []string,
	totalTimeChannel chan float64,
) {
	errors := []string{}
	latencies := []float64{}
	rrInconsistencies := 0
	wrInconsistencies := 0

	lambdaClient := lambda.New(
		session.New(),
		&aws.Config{Region: aws.String(endpoints.UsEast1RegionID)},
	)

	pyld := map[string]int{"count": 1}
	payload, _ := json.Marshal(pyld)
	input := &lambda.InvokeInput{
		FunctionName:   aws.String("ddb-txn"),
		Payload:        payload,
		InvocationType: aws.String("RequestResponse"),
	}

	benchStart := time.Now()
	requestId := int64(0)
	for ; requestId < threadRequestCount; requestId++ {
		requestStart := time.Now()
		response, err := lambdaClient.Invoke(input)
		requestEnd := time.Now()

		// Log the elapsed request time.
		latencies = append(latencies, float64(requestEnd.Sub(requestStart).Seconds()))

		// First, we check if the request itself returned an error. This should be
		// very unlikely.
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			// Next, we try to parse the response.
			bts := response.Payload
			inconsistencies := []int{}
			err = json.Unmarshal(bts, &inconsistencies)

			if len(inconsistencies) < 2 {
				errors = append(errors, string(bts))
			} else {
				wrInconsistencies += inconsistencies[0]
				rrInconsistencies += inconsistencies[1]
			}
		}
	}

	fmt.Println("Write-read inconsistencies: ", wrInconsistencies)
	fmt.Println("Read-read inconsistencies: ", rrInconsistencies)

	benchEnd := time.Now()
	totalTime := benchEnd.Sub(benchStart).Seconds()

	latencyChannel <- latencies
	errorChannel <- errors
	totalTimeChannel <- totalTime
}

// A function that uses a Go gRPC client to interact directly with Aft rather
// than using a Lambda.
func benchmarkLocal(
	tid int,
	threadRequestCount int64,
	replicas []string,
	latencyChannel chan []float64,
	errorChannel chan []string,
	totalTimeChannel chan float64,
) {
	errors := []string{}
	latencies := []float64{}

	clients := []*pb.AftClient{}
	for _, replica := range replicas {
		conn, err := grpc.Dial(fmt.Sprintf("%s:7654", replica), grpc.WithInsecure())
		if err != nil {
			fmt.Printf("Unexpected error:\n%v\n", err)
			os.Exit(1)
		}

		client := pb.NewAftClient(conn)
		clients = append(clients, &client)
		// defer conn.Close()
	}

	// Generate a new random source and create a Zipfian distribution.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(r, 1.1, 1.0, 999)

	writeData := make([]byte, 4096)
	r.Read(writeData)

	requestId := int64(0)
	benchStart := time.Now()
	errored := false

	for ; requestId < threadRequestCount; requestId++ {
		var err error
		keyCount := 6 // Set the number of keys accessed in this txn.
		writeCount := 2

		requestStart := time.Now()
		client := clients[rand.Intn(len(clients))] // Pick a client to use at random.
		tag, err := (*client).StartTransaction(context.Background(), &empty.Empty{})
		fmt.Println(err)

		for keyId := 0; keyId < keyCount; keyId++ {
			key := strconv.FormatUint(zipf.Uint64(), 10)

			update := &pb.KeyRequest{Tid: tag.Id}
			pair := &pb.KeyRequest_KeyPair{Key: key}

			update.Pairs = append(update.Pairs, pair)

			// Do the two writes first, then do the 4 reads.
			if keyId < writeCount {
				pair.Value = writeData
				_, err = (*client).Write(context.Background(), update)

				if err != nil {
					errored = true
					break
				}
			} else {
				_, err = (*client).Read(context.Background(), update)

				if err != nil {
					errored = true
					break
				}
			}
		}

		if !errored {
			tag, err = (*client).CommitTransaction(context.Background(), tag)
		}
		requestEnd := time.Now()

		if err != nil {
			errors = append(errors, err.Error())
		} else {
			latencies = append(latencies, float64(requestEnd.Sub(requestStart).Seconds()))
		}
	}

	benchEnd := time.Now()
	totalTime := benchEnd.Sub(benchStart).Seconds()

	latencyChannel <- latencies
	errorChannel <- errors
	totalTimeChannel <- totalTime
}
