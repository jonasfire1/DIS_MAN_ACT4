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
	Deferred   map[string]*pb.Message
	Replies    map[string]bool
}

func main() {

	if len(os.Args) < 2 {
		log.Fatal("Run the client with: go run ./client <node_identifier>")
	}

	node := &Node{
		Clients:  make(map[string]pb.RicartArgawalaClient),
		Deferred: make(map[string]*pb.Message),
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
	cli_num := 0
	for {
		for _, client := range node.Clients {
			_, err := client.Request(context.Background(), nil)
			if err == nil {
				cli_num++
				break
			}

			log.Println("Node down, retrying...")
			time.Sleep(time.Second)
		}
		if cli_num == len(node.Clients) {
			break
		}
		cli_num = 0
	}
	node.main()
}

func (n *Node) main() { // call this with a Node
	ctx := context.Background()
	for {
		n.Access(ctx)
		n.Work()
	}
}

func (n *Node) Work() {
	log.Println("accessed critical section")
	n.Clock++
	n.State = "RELEASED"
	log.Println("releasing critical section")
}

func (n *Node) Reply(ctx context.Context, msg *pb.Message) (*pb.Empty, error) { //respond to request use on node that made request
	if msg == nil || ctx == nil {
		return nil, nil
	}
	n.Clock = max(n.Clock, msg.GetClock()) + 1
	n.Mutex.Lock()
	n.Replies[msg.GetIdentifier()] = true
	n.Mutex.Unlock()

	return &pb.Empty{}, nil
}

func (n *Node) Request(ctx context.Context, msg *pb.Message) (*pb.Empty, error) { //request this from another node
	if msg == nil || msg.GetIdentifier() == "" || ctx == nil {
		return nil, nil
	}
	n.Clock = max(n.Clock, msg.GetClock()) + 1
	requester := n.Clients[msg.GetIdentifier()]
	MSG := pb.Message{
		Clock:      n.Clock,
		Identifier: n.identifier,
	} //new message
	switch n.State {

	case "RELEASED":
		n.Clients[msg.Identifier].Reply(ctx, &MSG)

	case "HELD":
		n.Deferred[msg.Identifier] = msg

	case "WANTED":
		if n.Clock < msg.Clock || (n.Clock == msg.Clock && n.identifier < msg.Identifier) {
			n.Deferred[msg.Identifier] = msg
		} else {
			log.Printf("msg=%+v\n", msg)
			log.Printf("identifier=%q\n", msg.GetIdentifier())
			log.Printf("requester nil? %v\n", requester == nil)
			log.Printf("ctx nil? %v\n", ctx == nil)
			log.Printf("MSG=%+v\n", MSG)
			requester.Reply(ctx, &MSG) //failure here but why msg is never nil nor is its vars and ctx isnt nil
		}
	}
	return nil, nil
}

func (n *Node) Access(ctx context.Context) {
	if ctx == nil {
		return
	}
	n.Clock++
	n.State = "WANTED"

	msg := &pb.Message{
		Clock:      n.Clock,
		Identifier: n.identifier,
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
