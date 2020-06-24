# AFT: A Serverless Fault-Tolerance Shim

[AFT](http://vikrams.io/papers/aft-eurosys20.pdf) is a serverless fault-tolerance and consistency shim that interposes between serverless compute and storage layers. By default, most serverless platforms (e.g., AWS Lambda, Cloudburst) provide blind retries in the face of function failures. However, functions that write multiple updates to storage can expose partial updates between their writes, which is exacerbated when functions are being retried.

To solve this problem, AFT enforces coordination-free transactions using the _read atomic_ consistency guarantee. Intuitively, read atomicity guarantees that a transaction reads data from a snapshot of committed transactions, respecting the order of commit. However, since the protocol is coordination free, a client is not guaranteed to see data from _all_ committed transactions -- only the transactions it is aware of. If you are interested in learning more about the details of the system, please see our [EuroSys20 paper](http://vikrams.io/papers/aft-eurosys20.pdf).

## Implementation

There are currently two versions of AFT that are implemented in this repository. The version of the system that is benchmarked in the paper can be found in `cmd/aft`, and it uses gRPC as a frontend. There is also a version (with the same guarantees) that is compatible with Cloudburst, deploying as that system's caching layer -- the code is in `cmd/aft_ipc`. This version includes a distributed metadata sharing and commit protocol that allows Cloudburst DAGs to run on multiple nodes while still seeing the benefits of read atomic consistency.

### Code Organization

The `cmd` directory, in addition to `aft` and `aft_ipc` includes a garbage collection server (`gc/`), a custom load balancer (`lb/`), and a benchmarking rig (`benchmark/`). All of these can be compiled using `make`. 

The `lib/consistency` directory contains a pluggable consistency framework for anyone interested in new consistency modes. `consistency/read_atomic.go` has the primary read atomicity implementation, and `consistency/lww.go` has a naive last writer wins implementation for reference. `lib/storage` has a pluggable storage backend interface and implementations for Redis, AWS DynamoDB, AWS S3, and Anna.

The `proto` directory has AFT-specific protobuf definitions and compiled protobufs for AFT (and Anna if necessary).

The `cluster` directory has scripts and YAML specs to spin up Kubernetes clusters that run multi-node versions of AFT. This is the recommended way to deploy the system.

### Other Notes

For those interested in running AFT with Cloudburst, note that Cloudburst support for AFT is not merged into the master branch of [the repository](https://github.com/hydro-project/cloudburst). This is because it would require significant code re-organization that we have not yet undertaken. For those interested in running Cloudburst clusters with AFT enabled, you can find the code for cluster spin-up (with only minor changes to the existing code) in [this branch](https://github.com/vsreekanti/cluster/tree/aft-cb) of the cluster repository and [this branch](https://github.com/vsreekanti/cloudburst/tree/aft-support) of the Cloudburst repository.