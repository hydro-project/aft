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

all: ipc_server server benchmark gc lb

ipc_server:
	cd cmd/aft_ipc && go build

server:
	cd cmd/aft && go build

benchmark:
	cd cmd/benchmark && go build

gc:
	cd cmd/gc && go build 
	cd cmd/gc/server && go build

lb:
	cd cmd/lb && go build

.PHONY: ipc_server server benchmark gc lb
