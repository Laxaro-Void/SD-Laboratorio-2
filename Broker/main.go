package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	dsRegisterMap "Broker/dsRegisterMap"
	dsUUID "Broker/dsUUID"
	dsConextionManager "Broker/dsConexionManager"
	pbRegisterAuth "Broker/proto/pbRegisterAuth"
	pbConsumer "Broker/proto/pbConsumer"
)

type Broker struct {
	// Registered users and productors
	trustedConexions *dsRegisterMap.RegisterMap
	authenticatedConexions *dsRegisterMap.RegisterMap

	bancoUSM  *dsConextionManager.BancoUSM
	dynamoDBClient *dsConextionManager.DynamoDB
}

type RegisterAuthServer struct {
	pbRegisterAuth.UnimplementedRegisterAuthServer
	broker *Broker
}

type ConsumerServer struct {
	pbConsumer.UnimplementedConsumerServer
	broker *Broker
}

/*
Registra una nueva conexion al servidor
*/
func (s *RegisterAuthServer) Register(ctx context.Context, req *pbRegisterAuth.RegisterRequest) (*pbRegisterAuth.RegisterResponse, error) {
	if (req.Type != "consumer" && req.Type != "producer" && req.Type != "banco" && req.Type != "DB") {
		log.Println("Invalid type. Must be 'consumer' or 'producer'")
		return &pbRegisterAuth.RegisterResponse{
			Success: false,
			Message: "Invalid type. Must be 'consumer' or 'producer'",
		}, nil
	}

	key := dsUUID.NewUUID()
	for s.broker.trustedConexions.Exists(key.String()) {
		key = dsUUID.NewUUID()
	}
	s.broker.trustedConexions.Add(key.String(), req.Username)

	log.Println(req.Username + " Register successfuly")
	return &pbRegisterAuth.RegisterResponse{
		Uuid:    key.String(),
		Success: true,
		Message: "Registration successful",
	}, nil
}

/*
Authenticate()

Autentica a un producto o consumidor registrado en el sistema.
*/
func (s *RegisterAuthServer) Authenticate(ctx context.Context, req *pbRegisterAuth.AuthRequest) (*pbRegisterAuth.AuthResponse, error) {
	if !s.broker.trustedConexions.Exists(req.Uuid) {
		log.Println(req.Uuid + " is not registered")
		return &pbRegisterAuth.AuthResponse{
			Success: false,
			Message: "You are not registered",
		}, nil
	}

	if s.broker.authenticatedConexions.Exists(req.Uuid) {
		return &pbRegisterAuth.AuthResponse{
			Success: true,
			Message: "You are already Authenticated",
		}, nil
	}

	s.broker.authenticatedConexions.Add(req.Uuid, req.Username)
	log.Println(req.Username + " Authenticated successful")

	return &pbRegisterAuth.AuthResponse{
		Success: true,
		Message: "Authenticated successful",
	}, nil
}

func (s *RegisterAuthServer) RegisterNode(ctx context.Context, req *pbRegisterAuth.RegisterNodeRequest) (*pbRegisterAuth.RegisterNodeResponse, error) {
	if !s.broker.trustedConexions.Exists(req.Uuid) {
		log.Println("Recive a Untrusted Node Request")
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes: false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.authenticatedConexions.Exists(req.Uuid) {
		log.Println("Recive a not authenticated Node Request")
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes: false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.dynamoDBClient.AddConection(req.Direction) {
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes: false,
			Message: "Fail to create Conection",
		}, nil
	}

	return &pbRegisterAuth.RegisterNodeResponse{
		Succes: true,
		Message: "Conection Stable",
	}, nil
}

func (s *RegisterAuthServer) RegisterBanco(ctx context.Context, req *pbRegisterAuth.RegisterNodeRequest) (*pbRegisterAuth.RegisterNodeResponse, error) {
	if !s.broker.trustedConexions.Exists(req.Uuid) {
		log.Println("Recive a Untrusted Node Request")
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes: false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.authenticatedConexions.Exists(req.Uuid) {
		log.Println("Recive a not authenticated Node Request")
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes: false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.bancoUSM.Connect(req.Direction) {
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes: false,
			Message: "Fail to create Conection",
		}, nil
	}

	return &pbRegisterAuth.RegisterNodeResponse{
		Succes: true,
		Message: "Conection Stable",
	}, nil

}

func ValidateEvent() {

}

func TicketsGenerator() {

}

func DataStorage() {

}

func DataRestore() {

}

func StartRegisterAuthServer(listener net.Listener, broker *Broker) {
	RegisterAuthServer := &RegisterAuthServer{
		broker: broker,
	}

	grpcServer := grpc.NewServer()
	pbRegisterAuth.RegisterRegisterAuthServer(grpcServer, RegisterAuthServer)

	log.Printf("RegisterAuthServer is listening on %s", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func serverBackground() {
	broker := &Broker{
		trustedConexions:      dsRegisterMap.NewRegisterMap(),
		authenticatedConexions:   dsRegisterMap.NewRegisterMap(),
		
		bancoUSM: new(dsConextionManager.BancoUSM),
		dynamoDBClient: new(dsConextionManager.DynamoDB),
	}

	broker.dynamoDBClient.N = 3
	broker.dynamoDBClient.R = 2
	broker.dynamoDBClient.W = 2
	broker.dynamoDBClient.Nodes = make(map[string]dsConextionManager.Node)

	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go StartRegisterAuthServer(listener, broker)

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
