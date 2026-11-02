package 

import (
	"context"
	"errors"
	pb "handin4/gRPC"
)

type Node struct {
	pb.UnimplementedChitchatServer

	Clients map[string]*Client
	Clock   int32
	Mutex   sync.Mutex
}