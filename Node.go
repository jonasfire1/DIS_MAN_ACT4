package main

import (
	"context"
	"net"
	"os"
	"time"

	pb "handin4/gRPC"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedRicartArgawalaServer
	Clock      int32
	Mutex      sync.Mutex
	Clients    map[string]pb.RicartArgawalaClient
	identifier string
	State      string
	Deferred   map[string]bool
	Replies    map[string]bool
}

func main() {

	if len(os.Args) < 2 {
		log.Fatal("Run the client with: go run ./client <node_identifier>")
	}

	node := &Node{
		Clients:  make(map[string]pb.RicartArgawalaClient),
		Deferred: make(map[string]bool),
	}
	var node_address string

	switch os.Args[1] {
	case "first_node":
		node.identifier = "first_node"
		node_address = ":5050"

		conn2, err := grpc.NewClient(
			"localhost:5051",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["second_node"] = pb.NewRicartArgawalaClient(conn2)

		conn3, err := grpc.NewClient(
			"localhost:5052",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["third_node"] = pb.NewRicartArgawalaClient(conn3)

	case "second_node":
		node.identifier = "second_node"
		node_address = ":5051"

		conn1, err := grpc.NewClient(
			"localhost:5050",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["first_node"] = pb.NewRicartArgawalaClient(conn1)

		conn3, err := grpc.NewClient(
			"localhost:5052",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["third_node"] = pb.NewRicartArgawalaClient(conn3)
	case "third_node":
		node.identifier = "third_node"
		node_address = ":5052"

		conn1, err := grpc.NewClient(
			"localhost:5050",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["first_node"] = pb.NewRicartArgawalaClient(conn1)

		conn2, err := grpc.NewClient(
			"localhost:5051",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["second_node"] = pb.NewRicartArgawalaClient(conn2)
	default:
		log.Fatal("the provided node identifier is invalid")
	}

	listener, err := net.Listen("tcp", node_address)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterRicartArgawalaServer(grpcServer, node)

	go func() {
		log.Println("Server SERVER_STARTED Port=" + node_address)

		if err := grpcServer.Serve(listener); err != nil {
			log.Println(err)
		}
	}()

	// this is supposed to be changed later
	test_message := &pb.Message{Clock: 0, Identifier: node.identifier}
	for {
		var approvals sync.WaitGroup
		approvals.Add(2) // needs 2 other approval responses
		for key := range node.Clients {
			go func() {
				defer approvals.Done()
				log.Println("sent message to " + key)
				test_message, err = node.Clients[key].Request(context.Background(), test_message, grpc.WaitForReady(true))
				if err != nil {
					log.Fatalf("Not working")
				}
				log.Println("recieved response from " + key)
			}()
		}
		approvals.Wait() // waits for approvals before proceding
		log.Println("both other nodes responded!")
	}
}

func (n *Node) Reply(ctx context.Context, msg *pb.Message) (*pb.Empty, error) { //respond to request use on node that made request
	//remake this to have reply code
	return nil, nil
}

func (n *Node) Request(ctx context.Context, msg *pb.Message) (*pb.Empty, error) { //request this from another node
	if n.State == "WANTED" || n.State == "RELEASED" {
		n.Deferred[msg.Identifier] = true
	} else {
		requester := n.Clients[msg.GetIdentifier()]
		MSG := pb.Message{
			Clock:      n.Clock,
			Identifier: n.identifier,
		} //new message
		requester.Reply(ctx, &MSG)
	}
	return nil, nil
}

func (n *Node) Access(ctx context.Context) {
	n.State = "WANTED"

	msg := &pb.Message{
		Clock:      n.Clock,
		Identifier: n.identifier,
		State:      "WANTED",
	}
	n.Replies = make(map[string]bool)
	for _, client := range n.Clients {
		client.Request(ctx, msg)
	}
	for {
		n.Mutex.Lock()
		repliesnum := len(n.Replies)
		n.Mutex.Unlock()

		if repliesnum == len(n.Clients) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	log.Println("all nodes responded")
	n.State = "HELD"
}
