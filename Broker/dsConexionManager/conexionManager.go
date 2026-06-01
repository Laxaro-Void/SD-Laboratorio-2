package dsConexionManager

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"

	pbDynamoDB "Broker/proto/pbDynamoDB"
	pbBancoUSM "Broker/proto/pbBancoUSM"
)


type Node struct {
	direction string
	connection *grpc.ClientConn
	client pbDynamoDB.DynamoDBClient
	state string
	isSync bool
}

type DynamoDB struct {
	// COnfiguration
	N int
	W int
	R int

	Nodes map[string]Node

	mu sync.Mutex
}

func (s *DynamoDB) AddConection(direction string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Nodes) < s.N {
		connection := NewGRPCClient(direction)
		s.Nodes[direction] = Node{
			direction: direction,
			connection: connection,
			client: pbDynamoDB.NewDynamoDBClient(connection),
			state: "Connected",
			isSync: false,
		}
		return true
	}

	for key, node := range s.Nodes {
		if node.state != "Connected" {
			delete(s.Nodes, key)
			connection := NewGRPCClient(direction)
			s.Nodes[direction] = Node{
				direction: direction,
				connection: connection,
				client: pbDynamoDB.NewDynamoDBClient(connection),
				state: "Connected",
				isSync: false,
			}
			return true
		}
	}

	if len(s.Nodes) >= s.N {
		log.Println("Aldready have tha maximun amount of nodes")
		return false
	}

	log.Fatalln("How did I get here?")
	return false
}

func (s *DynamoDB) ChangeStatusNode(direction string, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node := s.Nodes[direction]
	node.state = status
	s.Nodes[direction] = node
}

// Must be call using a go function
func (s *DynamoDB) CheckIsAliveProcedure() {
	for {
		time.Sleep(1 * time.Minute)

		for direction, node := range s.Nodes {
			go func(direction string, node Node) {
				s.mu.Lock()
				defer s.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel();
				res, err := node.client.CheckIsAlive(ctx, &pbDynamoDB.Empty{})
				if err != nil || !res.IsAlive {
					log.Printf("Failed to connect to %s: %v\n", direction, err)
					s.ChangeStatusNode(direction, "Disconnected")
					return
				}
			}(direction, node)
			
		}
	}
}

type BancoUSM struct {
	direction string
	connection *grpc.ClientConn
	client pbBancoUSM.BancoUSMClient
	state string
	mu sync.Mutex
}

func (s *BancoUSM) Connect(direction string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == "Connected" {
		log.Println("Aldready Connected to a alive BancoUSM")
		return false
	}

	if s.connection != nil {
		s.connection.Close()
	}

	s.connection = NewGRPCClient(direction)
	s.client = pbBancoUSM.NewBancoUSMClient(s.connection)
	s.state = "Connected"

	log.Println("Connected to BancoUSM :D")
	return true
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
}
