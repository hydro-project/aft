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

FROM ubuntu:18.04

MAINTAINER Vikram Sreekanti <vsreekanti@gmail.com> version: 0.1

USER root
ENV GOPATH /go
ENV AFT_HOME $GOPATH/src/github.com/vsreekanti/aft

# Setup the go dir.
RUN mkdir $GOPATH
RUN mkdir $GOPATH/bin
RUN mkdir $GOPATH/src
RUN mkdir $GOPATH/pkg

# Install Go, other Ubuntu dependencies.
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:longsleep/golang-backports
RUN apt-get update
RUN apt-get install -y golang-go wget unzip git ca-certificates net-tools python3-pip libzmq3-dev curl apt-transport-https

# Install kubectl for the management pod.
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update
RUN apt-get install -y kubectl

# Updates certificates, so go get works.
RUN update-ca-certificates

# Install protoc.
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v3.10.0/protoc-3.10.0-linux-x86_64.zip
RUN unzip protoc-3.10.0-linux-x86_64.zip -d /usr/local

# Clone the aft code.
RUN mkdir -p $GOPATH/src/github.com/vsreekanti
WORKDIR $AFT_HOME/..
RUN git clone https://github.com/vsreekanti/aft
WORKDIR $AFT_HOME

# Install required Go dependencies.
RUN go get -u google.golang.org/grpc
RUN go get -u github.com/golang/protobuf/protoc-gen-go
ENV PATH $PATH:$GOPATH/bin
RUN which protoc-gen-go

# Fetch the most recent version of the code and install dependencies.
WORKDIR $AFT_HOME/proto/aft
RUN protoc -I . aft.proto --go_out=plugins=grpc:.
WORKDIR $AFT_HOME
# RUN go get -u github.com/googleapis/gnostic 
RUN go get -d ./...
RUN cd $GOPATH/src/github.com/googleapis/gnostic && git checkout v0.4.0
# RUN go get -u -d k8s.io/klog
RUN cd $GOPATH/src/k8s.io/klog && git checkout v0.4.0

# Install Python dependencies.
RUN pip3 install zmq kubernetes boto3

WORKDIR $AFT_HOME
RUN cd proto/aft && protoc -I . aft.proto --go_out=plugins=grpc:.
WORKDIR /
COPY start-aft.sh /start-aft.sh

CMD bash start-aft.sh
