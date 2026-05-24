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

type BancoConnection struct {
	direction *string
	connection *grpc.ClientConn
	client pbRegisterAuth.RegisterAuthClient // change
	state string
}

func (c *BancoConnection) Connect() {
	if c.direction == nil {
		return
	}

	if c.state == "Disconected" {
		c.connection = NewGRPCClient(*c.direction)
		c.client = pbRegisterAuth.NewRegisterAuthClient(c.connection)
		c.state = "Connected"
	}
}



type BDConnection struct {
	direction *string
	connection *grpc.ClientConn
	client pbRegisterAuth.RegisterAuthClient // change
	state string
}

func (c *BDConnection) Connect() {
	if c.direction == nil {
		return
	}

	if c.state == "Disconected" {
		c.connection = NewGRPCClient(*c.direction)
		c.client = pbRegisterAuth.NewRegisterAuthClient(c.connection)
		c.state = "Connected"
	}
}

type Broker struct {
	// Registered users and productors
	registeredUsers      *dsRegisterMap.RegisterMap // uuid:username
	registeredProducer   *dsRegisterMap.RegisterMap // productorName:password
	registeredBanco		 *dsRegisterMap.RegisterMap
	registeredBD		 *dsRegisterMap.RegisterMap

	// Active sessions
	activeUserSessions 		 *dsRegisterMap.RegisterMap // uuid:username/productorName
	activeProducerSessions 	 *dsRegisterMap.RegisterMap // uuid:username/productorName
	activeBancoSessions 	 *dsRegisterMap.RegisterMap // uuid:username/productorName
	activeBDSessions 		 *dsRegisterMap.RegisterMap // uuid:username/productorName

	bancoUSM  *BancoConnection
	bdNodes []*BDConnection
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
	case "producer":
		register = s.broker.registeredProducer
	case "banco":
		register = s.broker.registeredBanco
	case "BD":
		register = s.broker.registeredBD
	default:
		log.Println("Invalid type. Must be 'consumer' or 'producer'")
		return &pbRegisterAuth.RegisterResponse{
			Success: false,
			Message: "Invalid type. Must be 'consumer' or 'producer'",
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
	if  !s.broker.registeredUsers.Exists(req.Uuid)      && 
		!s.broker.registeredProducer.Exists(req.Uuid)   && 
		!s.broker.registeredBanco.Exists(req.Uuid)      &&
		!s.broker.registeredBD.Exists(req.Uuid) {
		
		log.Println(req.Uuid + " is not registered")
		return &pbRegisterAuth.AuthResponse{
			Success: false,
			Message: "You are not registered",
		}, nil
	}

	var register *dsRegisterMap.RegisterMap
	if s.broker.registeredUsers.Exists(req.Uuid) {
		register = s.broker.activeUserSessions
	}   
	if s.broker.registeredProducer.Exists(req.Uuid) {
		register = s.broker.activeProducerSessions
	}
	if s.broker.registeredBanco.Exists(req.Uuid) {
		register = s.broker.activeBancoSessions
		if req.Direction == nil {
			return &pbRegisterAuth.AuthResponse{
				Success: false,
				Message: "Must Append Your Direction",
			}, nil
		}
		s.broker.bancoUSM.direction = req.Direction
	}
	if s.broker.registeredBD.Exists(req.Uuid) {
		register = s.broker.activeBDSessions
	}

	if register.Exists(req.Uuid) {
		return &pbRegisterAuth.AuthResponse{
			Success: true,
			Message: "You are already Authenticated",
		}, nil
	}

	register.Add(req.Uuid, req.Username)
	log.Println(req.Username + " Authenticated successful")

	return &pbRegisterAuth.AuthResponse{
		Success: true,
		Message: "Authenticated successful",
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
		registeredProducer:   dsRegisterMap.NewRegisterMap(),
		registeredBanco: 	  dsRegisterMap.NewRegisterMap(),
		registeredBD: 	      dsRegisterMap.NewRegisterMap(),

		activeUserSessions :	 dsRegisterMap.NewRegisterMap(),
		activeProducerSessions:  dsRegisterMap.NewRegisterMap(),
		activeBancoSessions: 	 dsRegisterMap.NewRegisterMap(),
		activeBDSessions:		 dsRegisterMap.NewRegisterMap(),
		
		
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

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
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
