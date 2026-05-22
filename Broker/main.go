package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	dsRegisterMap "Broker/dsRegisterMap"
	pbRegisterAuth "Broker/proto/pbRegisterAuth"
)

type Broker struct {
	// Registered users and productors
	registeredUsers      *dsRegisterMap.RegisterMap // username:password
	registeredProductors *dsRegisterMap.RegisterMap // productorName:password

	// Active sessions
	activeSessions map[string]string // sessionID:username/productorName

	// Event storage
	eventStorage []string // This can be replaced with a more complex structure for real events
}

type RegisterAuthServer struct {
	pbRegisterAuth.UnimplementedRegisterAuthServer
	broker *Broker
}

/*
Registra un nuevo producto o consumidor al sistema.
*/
func (s *RegisterAuthServer) Register(ctx context.Context, req *pbRegisterAuth.RegisterRequest) (*pbRegisterAuth.RegisterResponse, error) {
	var register *dsRegisterMap.RegisterMap
	switch req.Type {
	case "consumer":
		register = s.broker.registeredUsers
	case "productor":
		register = s.broker.registeredProductors
	default:
		log.Println("Invalid type. Must be 'consumer' or 'productor'")
		return &pbRegisterAuth.RegisterResponse{
			Success: false,
			Message: "Invalid type. Must be 'consumer' or 'productor'",
		}, nil
	}

	key := uuid.New().String()
	for register.Exists(key) {
		key = uuid.New().String()
	}
	register.Add(key, req.Username)

	log.Println(req.Username + " Register successfuly")
	return &pbRegisterAuth.RegisterResponse{
		Uuid:    key,
		Success: true,
		Message: "Registration successful",
	}, nil
}

/*
Authenticate()

Autentica a un producto o consumidor registrado en el sistema.
*/
func (s *RegisterAuthServer) Authenticate(ctx context.Context, req *pbRegisterAuth.AuthRequest) (*pbRegisterAuth.AuthResponse, error) {
	return &pbRegisterAuth.AuthResponse{
		Success: false,
		Message: "Not implemented yet",
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
		registeredUsers:      dsRegisterMap.NewRegisterMap(),
		registeredProductors: dsRegisterMap.NewRegisterMap(),
		activeSessions:       make(map[string]string),
		eventStorage:         make([]string, 0),
	}

	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go StartRegisterAuthServer(listener, broker)

	forever := make(chan bool)
	log.Printf("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	log.Printf("%s is running on port %s", name, port)

	serverBackground()
}
