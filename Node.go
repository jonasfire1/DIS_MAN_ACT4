package main

import (
	"context"

	pb "handin4/gRPC"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedRicartArgawalaServer

	Clock   int32
	Mutex   sync.Mutex
	Clients map[string]pb.RicartArgawalaClient
}

func main() {

	conn, err := grpc.NewClient(
		"localhost:5050",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

}

func Request(context.Context, *pb.Message) (*pb.Message, error) {

}
