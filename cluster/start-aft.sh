#!/bin/bash

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

IP=`ifconfig eth0 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`

# A helper function that takes a space separated list and generates a string
# that parses as a YAML list.
gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT        - $IP\n"
  done

  echo -e "$RESULT"
}

# Create the AWS access key infrastructure.
mkdir -p ~/.aws
echo -e "[default]\nregion = us-east-1" > ~/.aws/config
echo -e "[default]\naws_access_key_id = $AWS_ACCESS_KEY_ID\naws_secret_access_key = $AWS_SECRET_ACCESS_KEY" > ~/.aws/credentials

# Fetch the most recent version of the code.
cd $AFT_HOME
git checkout .
git fetch -p origin
git checkout -b brnch origin/$REPO_BRANCH
cd proto/aft
protoc -I . aft.proto --go_out=plugins=grpc:.
cd $AFT_HOME

# Build the most recent version of the code.

if [[ "$ROLE" = "manager" ]] || [[ "$ROLE" = "lb" ]]; then
  mkdir -p /root/.kube
fi

# Wait for the aft-config file to be passed in.
while [[ ! -f $AFT_HOME/config/aft-config.yml ]]; do
  X=1 # Empty command to pass.
done
cp $AFT_HOME/config/aft-base.yml $AFT_HOME/config/aft-config.yml

if [[ "$ROLE" != "bench" ]] && [[ "$ROLE" != "lb" ]]; then
  while [[ ! -f replicas.txt ]]; do
    X=1 # Empty command to pass.
  done

  REPLICA_IPS=`cat replicas.txt | awk 'BEGIN{ORS=" "}1'`
fi

# Generate the YML config file.
echo "ipAddress: $IP" >> config/aft-config.yml
echo "elbAddress: $ROUTE_ADDR" >> config/aft-config.yml
echo "managerAddress: $MGMT_IP" >> config/aft-config.yml
LST=$(gen_yml_list "$REPLICA_IPS")
echo "replicaList:" >> config/aft-config.yml
echo "$LST" >> config/aft-config.yml

# Start the process.
if [[ "$ROLE" = "aft" ]]; then
  make ipc_server
  ./cmd/aft_ipc/aft_ipc
elif [[ "$ROLE" = "manager" ]]; then
  make gc

  REPLICA_IPS=`cat ../replicas.txt | awk 'BEGIN{ORS=","}1'`
  GC_IPS=`cat ../gcs.txt | awk 'BEGIN{ORS=","}1'`

  python3 ft-server.py &

  ./cmd/gc/gc -replicaList $REPLICA_IPS -gcReplicaList $GC_IPS
elif [[ "$ROLE" = "bench" ]]; then
  make benchmark
  cd cmd/benchmark
  python3 benchmark_server.py
elif [[ "$ROLE" = "lb" ]]; then
  cd $GOPATH/src/k8s.io/klog
  git checkout v0.4.0

  cd $AFT_HOME
  make lb
  ./cmd/lb/lb
elif [[ "$ROLE" = "gc" ]]; then
  make gc

  ./cmd/gc/server/server
fi
