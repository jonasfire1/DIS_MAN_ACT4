package main

import (
	"context"
	"net"
	"os"

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
}

func main() {

	if len(os.Args) < 2 {
		log.Fatal("Run the client with: go run ./client <node_identifier>")
	}

	node := &Node{
		Clients: make(map[string]pb.RicartArgawalaClient),
	}

	var node_address string

	switch os.Args[1] {
	case "first_node":
		node.identifier = "first_node"
		node_address = ":5050"

		conn2, err := grpc.NewClient(
			"localhost:????",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["second_node"] = pb.NewRicartArgawalaClient(conn2)

		conn3, err := grpc.NewClient(
			"localhost:????",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["third_node"] = pb.NewRicartArgawalaClient(conn3)

	case "second_node":
		node.identifier = "second_node"
		node_address = ":????"

		conn1, err := grpc.NewClient(
			"localhost:5050",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["first_node"] = pb.NewRicartArgawalaClient(conn1)

		conn3, err := grpc.NewClient(
			"localhost:????",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["third_node"] = pb.NewRicartArgawalaClient(conn3)
	case "third_node":
		node.identifier = "third_node"
		node_address = ":????"

		conn1, err := grpc.NewClient(
			"localhost:5050",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		node.Clients["first_node"] = pb.NewRicartArgawalaClient(conn1)

		conn2, err := grpc.NewClient(
			"localhost:????",
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

}

func Request(context.Context, *pb.Message) (*pb.Message, error) {

}
