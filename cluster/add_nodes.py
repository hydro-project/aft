#!/usr/bin/env python3

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

import random
import os

import boto3

import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def add_nodes(client, apps_client, cfile, kinds, counts, management_ip,
              aws_key_id=None, aws_key=None, create=False, prefix=None):
    for i in range(len(kinds)):
        print('Adding %d %s server node(s) to cluster...' %
              (counts[i], kinds[i]))

        prev_count = util.get_previous_count(client, kinds[i])
        util.run_process(['./modify_ig.sh', kinds[i], str(counts[i] +
                                                          prev_count)])

    util.run_process(['./validate_cluster.sh'])
    replica_str = ' '.join(util.get_node_ips(client, 'role=aft'))

    # Create should only be true when the DaemonSet is being created for the
    # first time -- i.e., when this is called from create_cluster. After that,
    # we can basically ignore this because the DaemonSet will take care of
    # adding pods to created nodes.
    if create:
        for i in range(len(kinds)):
            kind = kinds[i]

            fname = 'yaml/ds/%s-ds.yml' % kind
            yml = util.load_yaml(fname, prefix)

            for container in yml['spec']['template']['spec']['containers']:
                env = container['env']
                util.replace_yaml_val(env, 'REPLICA_IPS', replica_str)
                util.replace_yaml_val(env, 'MANAGER', management_ip)
                util.replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
                util.replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)

            apps_client.create_namespaced_daemon_set(namespace=util.NAMESPACE,
                                                     body=yml)

            # Wait until all pods of this kind are running
            res = []
            while len(res) != counts[i]:
                res = util.get_pod_ips(client, 'role='+kind, is_running=True)

            created_pods = []
            pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                              label_selector='role=' +
                                              kind).items

            # Generate list of all recently created pods.
            for pod in pods:
                pname = pod.metadata.name
                for container in pod.spec.containers:
                    cname = container.name
                    created_pods.append((pname, cname))
                    pod.metadata.labels['aftReady'] = 'isready'
                    client.patch_namespaced_pod(pod.metadata.name,
                                                util.NAMESPACE, pod)

            # Copy the KVS config into all recently created pods.
            os.system('cp %s ./aft-config.yml' % cfile)

            for pname, cname in created_pods:
                util.copy_file_to_pod(client, 'aft-config.yml', pname,
                                      '/go/src/github.com/vsreekanti/aft/config', cname)
            os.system('rm ./aft-config.yml')
