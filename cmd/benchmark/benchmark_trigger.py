#!/usr/bin/env python3

import argparse
import datetime
import sys

import zmq

def main():
    parser = argparse.ArgumentParser(description='Starts an Aft benchmark.')
    parser.add_argument('-r', '--numRequests', nargs=1, type=int, metavar='R',
                        help='The number of requests to run per thread.',
                        dest='requests', required=True)
    parser.add_argument('-t', '--numThreads', nargs=1, type=int, metavar='T',
                        help='The number of threads per server to run.',
                        dest='threads', required=True)
    parser.add_argument('-rp', '--replicas', nargs=1, type=str, metavar='P',
                        help='A comma-separated list of Aft replicas',
                        required=True)
    parser.add_argument('-tp', '--threadPerServer', nargs=1, type=int,
                        metavar='U', help='The number of benchmark servers' +
                        'per machine to contact', dest='tpm', required=True)
    parser.add_argument('-nr', '--numReads', nargs=1, type=int,
                        metavar='R', dest='nr', required=False, default=1)
    parser.add_argument('-l', '--length', nargs=1, type=int,
                        metavar='L', dest='len', required=False, default=2)

    args = parser.parse_args()

    servers = []
    with open('benchmarks.txt') as f:
        lines = f.readlines()
        for line in lines:
            servers.append(line.strip())

    print('Found %d servers:%s' % (len(servers), '\n\t-' + '\n\t-'.join(servers)))

    message = ('%d:%d:%s:%d:%d:aft') % (args.threads[0], args.threads[0] *
                              args.requests[0], args.replicas[0], args.nr[0],
                                        args.len[0])

    conns = []
    context = zmq.Context(1)
    print('Starting benchmark at %s' % (str(datetime.datetime.now())))
    for server in servers:
        for i in range(args.tpm[0]):
            conn = context.socket(zmq.REQ)
            address = ('tcp://%s:%d') % (server, 8000 + i)
            conn.connect(address)
            conn.send_string(message)

            conns.append(conn)

    for conn in conns:
        response = conn.recv_string()
        print(str(response))

    print('Finished!')

if __name__ == '__main__':
    main()
