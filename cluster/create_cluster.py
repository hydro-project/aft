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

import argparse
import os

import boto3

from add_nodes import add_nodes
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def create_cluster(replica_count, gc_count, lb_count, bench_count, cfile,
                   ssh_key, cluster_name, kops_bucket, aws_key_id, aws_key):
    prefix = './'
    util.run_process(['./create_cluster_object.sh', kops_bucket, ssh_key])

    client, apps_client = util.init_k8s()

    print('Creating management pod')
    management_spec = util.load_yaml('yaml/pods/management-pod.yml')
    env = management_spec['spec']['containers'][0]['env']
    util.replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
    util.replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)

    client.create_namespaced_pod(namespace=util.NAMESPACE,
                                 body=management_spec)
    management_ip = util.get_pod_ips(client, 'role=management',
                                    is_running=True)[0]

    print('Creating standby replicas...')
    util.run_process(['./modify_ig.sh', 'standby', '1'])
    util.run_process(['./validate_cluster.sh'])
    print('Creating %d load balancer, %d GC replicas...' % (lb_count, gc_count))
    add_nodes(client, apps_client, cfile, ['lb', 'gc'], [lb_count, gc_count],
              management_ip, aws_key_id, aws_key, True, prefix)

    lb_pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                         label_selector="role=lb").items
    kubecfg = os.path.join(os.environ['HOME'], '.kube/config')
    for pod in lb_pods:
        util.copy_file_to_pod(client, kubecfg, pod.metadata.name,
                              '/root/.kube', 'lb-container')

    replica_ips = util.get_node_ips(client, 'role=gc', 'ExternalIP')
    with open('gcs.txt', 'w') as f:
        for ip in replica_ips:
            f.write(ip + '\n')

    # Wait until the monitoring pod is finished creating to get its IP address
    # and then copy KVS config into the monitoring pod.
    print('Creating %d Aft replicas...' % (replica_count))
    add_nodes(client, apps_client, cfile, ['aft'], [replica_count],
              management_ip, aws_key_id, aws_key, True, prefix)
    util.get_pod_ips(client, 'role=aft')

    replica_ips = util.get_node_ips(client, 'role=aft', 'ExternalIP')
    with open('replicas.txt', 'w') as f:
        for ip in replica_ips:
            f.write(ip + '\n')

    os.system('cp %s aft-config.yml' % cfile)
    management_pname = management_spec['metadata']['name']
    management_cname = management_spec['spec']['containers'][0]['name']
    util.copy_file_to_pod(client, 'aft-config.yml', management_pname,
                          '/go/src/github.com/vsreekanti/aft/config',
                          management_cname)
    util.copy_file_to_pod(client, 'replicas.txt', management_pname,
                          '/go/src/github.com/vsreekanti/aft',
                          management_cname)
    util.copy_file_to_pod(client, 'gcs.txt', management_pname,
                          '/go/src/github.com/vsreekanti/aft',
                          management_cname)
    util.copy_file_to_pod(client, kubecfg, management_pname, '/root/.kube/',
                          management_cname)
    os.system('rm aft-config.yml')
    os.system('rm gcs.txt')

    # Copy replicas.txt to all Aft pods.
    aft_pod_list = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                              label_selector="role=aft").items
    aft_pod_list = list(map(lambda pod: pod.metadata.name, aft_pod_list))
    for pname in aft_pod_list:
        util.copy_file_to_pod(client, 'replicas.txt', pname,
                              '/go/src/github.com/vsreekanti/aft',
                              'aft-container')

    gc_pod_list = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                              label_selector="role=gc").items
    gc_pod_list = list(map(lambda pod: pod.metadata.name, gc_pod_list))
    for pname in gc_pod_list:
        util.copy_file_to_pod(client, 'replicas.txt', pname,
                              '/go/src/github.com/vsreekanti/aft',
                              'gc-container')
    os.system('rm replicas.txt')

    print('Adding %d benchmark nodes...' % (bench_count))
    add_nodes(client, apps_client, cfile, ['benchmark'], [bench_count],
              management_ip, aws_key_id, aws_key, True, prefix)

    print('Finished creating all pods...')

    print('Creating Aft service...')
    service_spec = util.load_yaml('yaml/services/aft.yml', prefix)
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(
          Filters=[{'Name': 'group-name',
                    'Values': [sg_name]}])['SecurityGroups'][0]

    print('Authorizing ports for Aft replicas...')
    permission = [{
        'FromPort': 7654,
        'IpProtocol': 'tcp',
        'ToPort': 7656,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }, {
        'FromPort': 7777,
        'IpProtocol': 'tcp',
        'ToPort': 7782,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    },{
        'FromPort': 8000,
        'IpProtocol': 'tcp',
        'ToPort': 8003,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    } ]

    ec2_client.authorize_security_group_ingress(GroupId=sg['GroupId'],
                                                IpPermissions=permission)
    print('Finished!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Creates a Hydro cluster
                                     using Kubernetes and kops. If no SSH key
                                     is specified, we use the default SSH key
                                     (~/.ssh/id_rsa), and we expect that the
                                     correponding public key has the same path
                                     and ends in .pub.

                                     If no configuration file base is
                                     specified, we use the default
                                     ($config/aft-base.yml).''')

    parser.add_argument('-r', '--replicas', nargs=1, type=int, metavar='R',
                        help='The number of Aft replicas to start with ' +
                        '(required)', dest='replicas', required=True)
    parser.add_argument('-g', '--gc', nargs=1, type=int, metavar='G',
                        help='The number of GC replicas to start with ' +
                        '(required)', dest='gc', required=True)
    parser.add_argument('-l', '--lb', nargs=1, type=int, metavar='L',
                        help='The number of LB replicas to start with ' +
                        '(required)', dest='lb', required=True)
    parser.add_argument('-b', '--benchmark', nargs='?', type=int, metavar='B',
                        help='The number of benchmark nodes in the cluster ' +
                        '(optional)', dest='benchmark', default=0)
    parser.add_argument('--conf', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='conf',
                        default='../config/aft-base.yml')
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'],
                                             '.ssh/id_rsa'))

    cluster_name = util.check_or_get_env_arg('HYDRO_CLUSTER_NAME')
    kops_bucket = util.check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    args = parser.parse_args()

    create_cluster(args.replicas[0], args.gc[0], args.lb[0], args.benchmark,
                   args.conf, args.sshkey, cluster_name, kops_bucket,
                   aws_key_id, aws_key)
