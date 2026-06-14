package main

import (
	"os"
	"net"
	"log"
	"fmt"
	"sort"
	"sync"
	"time"
	"context"

	"google.golang.org/grpc"

	pbBroker "DynamoDB/proto/pbBroker"
	pbDynamoDB "DynamoDB/proto/pbDynamoDB"
)

type Table struct {
	Name string
	Data map[string][]byte
}

type Node struct {
	Broker pbBroker.BrokerClient

	mu sync.Mutex
	Tables map[string]Table
}

type NodeServer struct {
	Node *Node
	pbDynamoDB.UnimplementedDynamoDBServer
}

func startServer(Node *Node) {
	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pbDynamoDB.RegisterDynamoDBServer(grpcServer, &NodeServer{
		Node: Node,
	})

	log.Printf("Servers is listening on %s", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *NodeServer) CheckIsAlive(ctx context.Context, req *pbDynamoDB.Empty) (*pbDynamoDB.IsAliveResponde, error) {
	return &pbDynamoDB.IsAliveResponde{
		IsAlive: true,
	}, nil
}

func (s *Node) Handshake() bool {
	conn := NewGRPCClient(os.Getenv("BROKER_URL"))
	s.Broker = pbBroker.NewBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	res, err := s.Broker.Handshake(ctx, &pbBroker.HandshakeRequest{
		Direction: os.Getenv("MYDIRECTION"),
		WhatIAm: "NODE",
	})
	if err != nil || !res.Success {
		log.Printf("Faild to do Handshake: %v", err)
		return false
	}

	_, err = s.Broker.CheckIsAlive(context.Background(), &pbBroker.Empty{})
	if err != nil {
		log.Printf("Faild to check Alive: %v", err)
		return false
	}

	log.Print("Conection Succsess")
	return true
}

func (s *NodeServer) SyncAllData(ctx context.Context, req *pbDynamoDB.SyncAllDataRequest) (*pbDynamoDB.SyncAllDataResponse, error) {
	s.Node.mu.Lock()
	defer s.Node.mu.Unlock()

	log.Println("[NODE] Syncronizing incoming Data")

	for k := range s.Node.Tables {
		delete(s.Node.Tables, k)
	}

	for _, table := range req.AllData {
		var newTable = make(map[string][]byte)

		for idx, key := range table.Keys {
			newTable[key] = table.Value[idx]
		}

		s.Node.Tables[table.Name] = Table{
			Name: table.Name,
			Data: newTable,
		}
	}

	return &pbDynamoDB.SyncAllDataResponse{
		Success: true,
		Message: "Sync Succes",
	}, nil
}

func (s *NodeServer) GetAllData(ctx context.Context, req *pbDynamoDB.Empty) (*pbDynamoDB.GetAllDataResponse, error) {
	s.Node.mu.Lock()
	defer s.Node.mu.Unlock()

	log.Println("[NODE] Recolecting full Data")

	res := &pbDynamoDB.GetAllDataResponse {
		AllData: make([]*pbDynamoDB.Table, 0),
	}

	for _, table := range s.Node.Tables {
		var keys []string
		var values [][]byte

		for key, value := range table.Data {
			keys = append(keys, key)
			values = append(values, value)
		}

		res.AllData = append(res.AllData, &pbDynamoDB.Table{
			Name: table.Name,
			Keys: keys,
			Value: values,
		})
	}

	log.Println("[NODE] Recolecting Ready")

	return res, nil
}

func (s *NodeServer) CreateTable(ctx context.Context, req *pbDynamoDB.CreateTableRequest) (*pbDynamoDB.CreateTableResponse, error) {
	s.Node.mu.Lock()
	defer s.Node.mu.Unlock()

	if _, exists := s.Node.Tables[req.TableName]; exists {
		return &pbDynamoDB.CreateTableResponse{
			Success: false,
			Message: "Table already exists",
		}, nil
	}

	s.Node.Tables[req.TableName] = Table{
		Name: req.TableName,
		Data: make(map[string][]byte),
	}

	log.Printf("Table %s created successfully", req.TableName)

	return &pbDynamoDB.CreateTableResponse{
		Success: true,
		Message: "Table created successfully",
	}, nil
}

func (s *NodeServer) PutItem(ctx context.Context, req *pbDynamoDB.PutItemRequest) (*pbDynamoDB.PutItemResponse, error) {
	s.Node.mu.Lock()
	defer s.Node.mu.Unlock()

	table, exists := s.Node.Tables[req.TableName]
	if !exists {
		return &pbDynamoDB.PutItemResponse{
			Success: false,
			Message: "Table does not exist",
		}, nil
	}

	table.Data[req.Key] = req.Value
	s.Node.Tables[req.TableName] = table
	var preview string
	if req.Value == nil {
		preview = "nil"
	} else if len(req.Value) > 3 {
		preview = fmt.Sprintf("%v...", req.Value[:3])
	} else {
		preview = fmt.Sprintf("%v", req.Value)
	}
	log.Printf("[NODE] Added item to table %s: key=%s, value=%s", req.TableName, req.Key, preview)

	return &pbDynamoDB.PutItemResponse{
		Success: true,
		Message: "Item added successfully",
	}, nil
}

func (s *NodeServer) GetTable(ctx context.Context, req *pbDynamoDB.GetTableRequest) (*pbDynamoDB.GetTableResponse, error) {
	s.Node.mu.Lock()
	defer s.Node.mu.Unlock()

	table, exists := s.Node.Tables[req.TableName]
	if !exists {
		return &pbDynamoDB.GetTableResponse{
			Success: false,
			Message: "Table does not exist",
			Data: nil,
		}, nil
	}

	var keys []string
	var values [][]byte

	for key := range table.Data {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	
	for _, k := range keys {
		values = append(values, table.Data[k])
	}

	log.Printf("[NODE] Got table %s", req.TableName)
	return &pbDynamoDB.GetTableResponse{
		Success: true,
		Message: "Item retrieved successfully",
		Data: &pbDynamoDB.Table{
				Name: table.Name,
				Keys: keys,
				Value: values,
			},
	}, nil
}

func (s *NodeServer) GetItem(ctx context.Context, req *pbDynamoDB.GetItemRequest) (*pbDynamoDB.GetItemResponse, error) {
	s.Node.mu.Lock()
	defer s.Node.mu.Unlock()

	table, exists := s.Node.Tables[req.TableName]
	if !exists {
		return &pbDynamoDB.GetItemResponse{
			Success: false,
			Message: "Table does not exist",
		}, nil
	}

	value, exists := table.Data[req.Key]

	if !exists {
		log.Printf("[NODE] Fail key %s Not Exist", req.Key)
		return &pbDynamoDB.GetItemResponse{
			Success: true,
			Message: "Key does not exist",
			Value: &pbDynamoDB.Item{
				Value: nil,
				Exists: exists,
			},
		}, nil
	}

	log.Printf("[NODE] Got item for key %s in table %s", req.Key, req.TableName)
	return &pbDynamoDB.GetItemResponse{
		Success: true,
		Message: "Item retrieved successfully",
		Value: &pbDynamoDB.Item{
				Value: value,
				Exists: exists,
			},
	}, nil
}

func serverBackground() {
	node := &Node {
		Tables: make(map[string]Table),
	}

	go startServer(node)
	time.Sleep(1 * time.Second)

	for !node.Handshake() {
		time.Sleep(2 * time.Second)
	}

	forever := make(chan bool)
	log.Printf("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", address, err)
	}
	return conn
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	log.Printf("%s is running on port %s", name, port)

	serverBackground()
}
