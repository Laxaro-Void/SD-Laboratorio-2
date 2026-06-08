package main

import (
	"bufio"
	"context"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	pbDynamoDB "DynamoDB/proto/pbDynamoDB"
	pbRegisterAuth "DynamoDB/proto/pbRegisterAuth"
)

type Table struct {
	Name string
	Data map[string][]byte
}

type Node struct {
	BrokerConnection   *grpc.ClientConn
	RegisterAuthClient pbRegisterAuth.RegisterAuthClient

	KeyPath string
	uuid    string

	mu   sync.Mutex
	Tables map[string]Table
}

type NodeServer struct {
	pbDynamoDB.UnimplementedDynamoDBServer
	node *Node
}

func (c *Node) SaveUUID(uuid string) {
	file, err := os.Create(c.KeyPath)
	if err != nil {
		log.Fatalf("Error Creating file: %v", err)
	}
	defer file.Close()

	file.WriteString(uuid + "\n")
	log.Println("Key Saved, Now you can safely close your sesion")
}

func (c *Node) ReadUUID() (string, bool) {
	_, err := os.Stat(c.KeyPath)
	if os.IsNotExist(err) {
		log.Println("Key do not exist")
		return "", false
	}

	file, err := os.Open(c.KeyPath)
	if err != nil {
		log.Fatalf("Error Opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan()

	uuid := scanner.Text()

	return uuid, true
}

func (c *Node) Register(username string) (string, bool) {
	message := pbRegisterAuth.RegisterRequest{
		Username: username,
		Type:     "DB",
	}

	response, err := c.RegisterAuthClient.Register(context.Background(), &message)
	if err != nil {
		log.Printf("Error registering Node: %v", err)
		return "", false
	}

	if !response.Success {
		log.Printf("Failed to register Node %s: %s", username, response.Message)
		return "", false
	}

	log.Printf("Node %s registered successfully: UUID=%s", username, response.Uuid)
	return response.Uuid, true
}

func (c *Node) Authenciate(username string, uuid string) bool {
	message := pbRegisterAuth.AuthRequest{
		Username: username,
		Uuid:     uuid,
	}

	response, err := c.RegisterAuthClient.Authenticate(context.Background(), &message)
	if err != nil {
		log.Printf("Error authenticating consumer: %v", err)
		return false
	}

	log.Println(response.Message)
	return response.Success
}

func (c *Node) RequestSession() bool {
	key, succes := c.ReadUUID()
	if succes {
		succes = c.Authenciate(os.Getenv("NAME"), key)
	}

	if !succes {
		for {
			key, succes = c.Register(os.Getenv("NAME"))
			if succes {
				break
			}
			log.Println("Retrining in 5 second...")
			time.Sleep(5 * time.Second)
		}
		c.SaveUUID(key)
	} else {
		return true
	}

	return c.Authenciate(os.Getenv("NAME"), key)
}

func (c *Node) RegisterNode() bool {
	uuid, _ := c.ReadUUID()
	message := &pbRegisterAuth.RegisterNodeRequest{
		Uuid:      uuid,
		Direction: os.Getenv("MYDIRECTION"),
	}

	response, err := c.RegisterAuthClient.RegisterNode(context.Background(), message)
	if err != nil {
		log.Fatalf("Error to Register Node, %v", err)
		return false
	}

	if !response.Succes {
		log.Fatalf("Broker regect the node: %s", response.Message)
		return false
	}

	log.Println("Borker Conected to Node")
	return true
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
}

func (s *NodeServer) CheckIsAlive(ctx context.Context, req *pbDynamoDB.Empty) (*pbDynamoDB.IsAliveResponde, error) {
	return &pbDynamoDB.IsAliveResponde{
		IsAlive: true,
	}, nil
}

func (s *NodeServer) CreateTable(ctx context.Context, req *pbDynamoDB.CreateTableRequest) (*pbDynamoDB.CreateTableResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	if _, exists := s.node.Tables[req.TableName]; exists {
		return &pbDynamoDB.CreateTableResponse{
			Success: false,
			Message: "Table already exists",
		}, nil
	}

	s.node.Tables[req.TableName] = Table{
		Name: req.TableName,
		Data: make(map[string][]byte),
	}

	return &pbDynamoDB.CreateTableResponse{
		Success: true,
		Message: "Table created successfully",
	}, nil
}

func (s *NodeServer) PutItem(ctx context.Context, req *pbDynamoDB.PutItemRequest) (*pbDynamoDB.PutItemResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	table, exists := s.node.Tables[req.TableName]
	if !exists {
		return &pbDynamoDB.PutItemResponse{
			Success: false,
			Message: "Table does not exist",
		}, nil
	}

	table.Data[req.Key] = req.Value
	s.node.Tables[req.TableName] = table

	return &pbDynamoDB.PutItemResponse{
		Success: true,
		Message: "Item added successfully",
	}, nil
}

func (s *NodeServer) GetItem(ctx context.Context, req *pbDynamoDB.GetItemRequest) (*pbDynamoDB.GetItemResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	table, exists := s.node.Tables[req.TableName]
	if !exists {
		return &pbDynamoDB.GetItemResponse{
			Success: false,
			Message: "Table does not exist",
		}, nil
	}

	value, exists := table.Data[req.Key]
	if !exists {
		return &pbDynamoDB.GetItemResponse{
			Success: false,
			Message: "Key does not exist",
		}, nil
	}

	return &pbDynamoDB.GetItemResponse{
		Success: true,
		Message: "Item retrieved successfully",
		Value:   value,
	}, nil
}

func StartNodeServer(listener net.Listener, node *Node) {
	NodeServer := &NodeServer{
		node: node,
	}

	grpcServer := grpc.NewServer()
	pbDynamoDB.RegisterDynamoDBServer(grpcServer, NodeServer)

	log.Printf("NodeServer is listening on %s", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func initNode() {
	BrokerConnection := NewGRPCClient(os.Getenv("BROKER_URL"))
	defer BrokerConnection.Close()

	node := &Node{
		BrokerConnection:   BrokerConnection,
		RegisterAuthClient: pbRegisterAuth.NewRegisterAuthClient(BrokerConnection),
		KeyPath:            "keys/" + os.Getenv("NAME") + "-key.txt",
	}

	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go StartNodeServer(listener, node)
	time.Sleep(2 * time.Second)

	if !node.RequestSession() {
		panic("Unable to request Session")
	}

	node.RegisterNode()
	log.Println("Conexion estable to broker")

	forever := make(chan bool)
	log.Printf("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	hostname, _ := os.Hostname()
	log.Printf("%s is running on port %s, Hostname: %s", name, port, hostname)

	initNode()
}
