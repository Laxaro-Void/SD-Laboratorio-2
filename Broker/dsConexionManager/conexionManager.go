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
	// Configuration
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

func (s *DynamoDB) CreateTable(tableName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Nodes) == 0 {
		log.Printf("Failed to create table %s: No nodes available\n", tableName)
		return false
	}

	success := 0

	for _, node := range s.Nodes {
		if node.state == "Connected" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel();
			res, err := node.client.CreateTable(ctx, &pbDynamoDB.CreateTableRequest{
				TableName: tableName,
			})
			if err != nil || !res.Success {
				log.Printf("Failed to create table %s on node %s: %v\n", tableName, node.direction, err)
			}
			success++
		} else {
			log.Printf("Failed to create table %s on node %s: Node is not connected\n", tableName, node.direction)
		}
	}

	if success < s.W {
		log.Printf("Failed to create table %s: Some nodes failed to create the table\n", tableName)
		return false
	}

	return true
}

func (s *DynamoDB) PutItem(tableName string, key string, data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Nodes) == 0 {
		log.Printf("Failed to put item in table %s: No nodes available\n", tableName)
		return false
	}

	success := 0

	for _, node := range s.Nodes {
		if node.state == "Connected" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel();
			res, err := node.client.PutItem(ctx, &pbDynamoDB.PutItemRequest{
				TableName: tableName,
				Key: key,
				Value: data,
			})
			if err != nil || !res.Success {
				log.Printf("Failed to put item in table %s on node %s: %v\n", tableName, node.direction, err)
			}
			success++
		} else {
			log.Printf("Failed to put item in table %s on node %s: Node is not connected\n", tableName, node.direction)
		}
	}

	if success < s.W {
		log.Printf("Failed to put item in table %s: Some nodes failed to put the item\n", tableName)
		return false
	}

	return true
}

func (s *DynamoDB) GetItem(tableName string, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Nodes) == 0 {
		log.Printf("Failed to get item from table %s: No nodes available\n", tableName)
		return nil, false
	}

	success := 0
	var resValues [][]byte

	for _, node := range s.Nodes {
		if node.state == "Connected" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel();
			res, err := node.client.GetItem(ctx, &pbDynamoDB.GetItemRequest{
				TableName: tableName,
				Key: key,
			})
			if err != nil || !res.Success {
				log.Printf("Failed to get item from table %s on node %s: %v\n", tableName, node.direction, err)
				continue
			}
			success++
			resValues = append(resValues, res.Value)
		} else {
			log.Printf("Failed to get item from table %s on node %s: Node is not connected\n", tableName, node.direction)
		}
	}

	if success < s.R {
		log.Printf("Failed to get item from table %s: Not enough successful reads for quorum\n", tableName)
		return nil, false
	}

	counts := make(map[string]int)
	values := make(map[string][]byte)

	for _, value := range resValues {
		valueKey := string(value)
		counts[valueKey]++
		if _, ok := values[valueKey]; !ok {
			values[valueKey] = value
		}
	}

	mostCount := 0
	var mostValue []byte
	for key, count := range counts {
		if count > mostCount {
			mostCount = count
			mostValue = values[key]
		}
	}

	if mostCount < s.R {
		log.Printf("Failed to get item from table %s: No value reached quorum of %d reads\n", tableName, s.R)
		return nil, false
	}

	return mostValue, true
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

func (s *BancoUSM) ProcessPayment(uuid string, amount int32, paymentMethod string) (bool, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != "Connected" {
		log.Println("Not connected to BancoUSM")
		return false, "Not connected to BancoUSM", nil
	}

	response, err := s.client.ProcessPayment(context.Background(), &pbBancoUSM.PaymentRequest{
		Uuid: uuid,
		Amount: amount,
		PaymentMethod: paymentMethod,
	})
	if err != nil {
		log.Printf("Error processing payment: %v", err)
		return false, "Error processing payment", err
	}

	return response.Success, response.Message, err
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
}
