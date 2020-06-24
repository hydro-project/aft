package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	zmq "github.com/pebbe/zmq4"
	"k8s.io/api/core/v1"
	v1meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	LbPort       = 8000
	PullTemplate = "tcp://*:%d"
)

func createSocket(tp zmq.Type, context *zmq.Context, address string, bind bool) *zmq.Socket {
	sckt, err := context.NewSocket(tp)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}

	if bind {
		err = sckt.Bind(address)
	} else {
		err = sckt.Connect(address)
	}

	if err != nil {
		fmt.Printf("Unexpected error while binding/connecting socket to %s:\n%v", address, err)
		os.Exit(1)
	}

	return sckt
}

func main() {
	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	config, err := clientcmd.BuildConfigFromFlags("", "/root/.kube/config")
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	addresses, err := getAddresses(client)
	if err != nil {
		fmt.Println("Unable to read address set. Exiting:\n", err)
		os.Exit(1)
	}

	lbSocket := createSocket(zmq.REP, context, fmt.Sprintf(PullTemplate, LbPort), true)
	poller := zmq.NewPoller()
	poller.Add(lbSocket, zmq.POLLIN)

	updateStart := time.Now()
	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		// We only have 1 socket, so no need to check which one it is.
		if len(sockets) > 0 {
			lbSocket.Recv(zmq.DONTWAIT) // We can ignore this message.

			index := rand.Intn(len(addresses))
			address := addresses[index]

			lbSocket.Send(address, zmq.DONTWAIT)
		}

		updateEnd := time.Now()
		if updateEnd.Sub(updateStart).Seconds() > 1.0 {
			newAddresses, err := getAddresses(client)
			if err != nil {
				fmt.Println("Unable to retrieve addresses. Not updating list...\n", err)
				continue
			}

			addresses = newAddresses
			updateStart = time.Now()
		}
	}
}

func getAddresses(client *kubernetes.Clientset) ([]string, error) {
	nodes, err := client.CoreV1().Nodes().List(context.TODO(), v1meta.ListOptions{LabelSelector: "role=aft"})
	if err != nil {
		return []string{}, err
	}

	addresses := make([]string, len(nodes.Items))
	for index, node := range nodes.Items {
		if checkNodeReady(client, getNodeAddress(node.Status.Addresses, v1.NodeHostName)) {
			addresses[index] = getNodeAddress(node.Status.Addresses, v1.NodeExternalIP)
		}
	}

	return addresses, nil
}

func getNodeAddress(addresses []v1.NodeAddress, tp v1.NodeAddressType) string {
	for _, address := range addresses {
		if address.Type == tp {
			return address.Address
		}
	}

	return ""
}

func checkNodeReady(client *kubernetes.Clientset, nodeName string) bool {
	pods, err := client.CoreV1().Pods("default").List(context.TODO(), v1meta.ListOptions{LabelSelector: "role=aft", FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName)})
	if err != nil || len(pods.Items) == 0 {
		fmt.Println("Unexpected error while checking pod status:\n", err)
		return false
	}

	pod := pods.Items[0]
	label, ok := pod.Labels["aftReady"]

	return ok && label == "isready"
}
