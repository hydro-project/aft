#!/usr/bin/env python3

import logging
import os
import tarfile
from tempfile import TemporaryFile
import time

import boto3
import kubernetes as k8s
from kubernetes.stream import stream

ec2 = boto3.client('ec2')
ec2r = boto3.resource('ec2')

logging.basicConfig(filename='log_ft.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

def main():
    while not os.path.isfile('../replicas.txt'):
        continue # Wait till the file is copied in.

    with open('../replicas.txt', 'r') as f:
        replicas = list(map(lambda line:  line.strip(), f.readlines()))

    cfg = k8s.config
    cfg.load_kube_config()
    client = k8s.client.CoreV1Api()

    while True:
        time.sleep(5)
        logging.info("Checking for errors...")

        # List all Aft nodes.
        current_replicas = list_all_aft_ips(client, True)

        if len(current_replicas) < len(replicas):
            logging.info("Found a potential fault: there are fewer replicas"
                         " than expected.")

            # Move a node over from standby.
            sb_nodes = client.list_node(label_selector='role=standby').items
            if len(sb_nodes) == 0:
                # wait for more standby nodes to come online
                logging.info("Looping on standby nodes.")
                continue

            node = sb_nodes[0]
            int_ip = None
            for address in node.status.addresses:
                if address.type == 'InternalIP':
                    int_ip = address.address

            node.metadata.labels['role'] = 'aft'
            client.patch_node(node.metadata.name, node)

            instance_id = ec2.describe_instances(Filters=[{'Name':
                                                           'network-interface.addresses.private-ip-address',
                                                           'Values':
                                                           [int_ip]}])['Reservations'][0]['Instances'][0]['InstanceId']
            instance = ec2r.Instance(instance_id)
            instance.delete_tags(Tags=[{'Key': 'Name'},
                                       {'Key': 'k8s.io/cluster-autoscaler/node-template/label/role'}])
            instance.create_tags(Tags=[
                {
                    'Key': 'Name',
                    'Value': 'aft-instances.ucbfluent.de'
                },
                {
                    'Key':
                    'k8s.io/cluster-autoscaler/node-template/label/role',
                    'Value': 'aft'
                }
            ])
            logging.info("Changed tags...")


            time.sleep(5) # wait for the new pod to come online
            setup_pod(client, int_ip)
        else:
            cr_set = set(current_replicas)
            cr_removed_set = set(cr_set)
            for replica in replicas:
                for cr in cr_set:
                    if cr[0] == replica:
                        cr_removed_set.remove(cr)

            if len(cr_removed_set) > 0:
                for node in cr_set:
                    setup_pod(client, node[1])
            else:
                logging.info("No faults detected.")

        replicas = list_all_aft_ips(client, False)


def list_all_aft_ips(client, include_internal=False):
    nodes = client.list_node(label_selector='role=aft').items
    current_replicas = []
    for node in nodes:
        ext_ip = None
        int_ip = None
        for address in node.status.addresses:
            if address.type == 'ExternalIP':
                ext_ip = address.address
            elif address.type == 'InternalIP':
                int_ip = address.address

        if include_internal:
            current_replicas.append((ext_ip, int_ip))
        else:
            current_replicas.append(ext_ip)

    return current_replicas


def setup_pod(client, internal_ip):
    logging.info('Setting up pod %s...' % internal_ip)
    pods = client.list_namespaced_pod(namespace='default',
                                      label_selector='role=aft').items

    pname = None
    aft_pod = None
    running = False

    while not running:
        for pod in pods:
            if pod.status.pod_ip == internal_ip:
                pname = pod.metadata.name
                aft_pod = pod
                if pod.status.phase == 'Running':
                    running = True # wait till the pod is running

    ips = list_all_aft_ips(client)
    with open('replicas.txt', 'w') as f:
        for ip in ips:
            f.write(ip + '\n')
    os.system('kubectl cp replicas.txt %s:/go/src/github.com/vsreekanti/aft' % pname)
    os.system('kubectl cp ../config/aft-config.yml %s:/go/src/github.com/vsreekanti/aft/config' % pname)
    os.system('rm replicas.txt')

    time.sleep(50)
    aft_pod.metadata.labels['aftReady'] = 'isready'
    aft_pod = client.patch_namespaced_pod(aft_pod.metadata.name, 'default', aft_pod)
    logging.info(aft_pod)

if __name__ == '__main__':
    main()
