package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	dsConextionManager "Broker/dsConexionManager"
	dsRegisterMap "Broker/dsRegisterMap"
	dsUUID "Broker/dsUUID"
	pbRegisterAuth "Broker/proto/pbRegisterAuth"
	pbProducer "Broker/proto/pbProducer"
	pbConsumer "Broker/proto/pbConsumer"
)

type Broker struct {
	// Registered users and productors
	trustedConexions       *dsRegisterMap.RegisterMap
	authenticatedConexions *dsRegisterMap.RegisterMap

	bancoUSM       *dsConextionManager.BancoUSM
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

type ProducerServer struct {
	pbProducer.UnimplementedProducerServer
	broker *Broker
}

type Event struct {
	Evento_id         string
	Discoteca         string
	Nombre_evento     string
	Categoria         string
	Comuna            string
	Precio            int
	Stock             int
	Fecha_evento      string
	Fecha_publicacion string
}

/*
Registra una nueva conexion al servidor
*/
func (s *RegisterAuthServer) Register(ctx context.Context, req *pbRegisterAuth.RegisterRequest) (*pbRegisterAuth.RegisterResponse, error) {
	if req.Type != "consumer" && req.Type != "producer" && req.Type != "banco" && req.Type != "DB" {
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
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.authenticatedConexions.Exists(req.Uuid) {
		log.Println("Recive a not authenticated Node Request")
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.dynamoDBClient.AddConection(req.Direction) {
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes:  false,
			Message: "Fail to create Conection",
		}, nil
	}

	return &pbRegisterAuth.RegisterNodeResponse{
		Succes:  true,
		Message: "Conection Stable",
	}, nil
}

func (s *RegisterAuthServer) RegisterBanco(ctx context.Context, req *pbRegisterAuth.RegisterNodeRequest) (*pbRegisterAuth.RegisterNodeResponse, error) {
	if !s.broker.trustedConexions.Exists(req.Uuid) {
		log.Println("Recive a Untrusted Node Request")
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.authenticatedConexions.Exists(req.Uuid) {
		log.Println("Recive a not authenticated Node Request")
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.bancoUSM.Connect(req.Direction) {
		return &pbRegisterAuth.RegisterNodeResponse{
			Succes:  false,
			Message: "Fail to create Conection",
		}, nil
	}

	return &pbRegisterAuth.RegisterNodeResponse{
		Succes:  true,
		Message: "Conection Stable",
	}, nil

}

func (s *ProducerServer) PublishEvent(ctx context.Context, req *pbProducer.PublishEventRequest) (*pbProducer.PublishEventResponse, error) {
	if !s.broker.trustedConexions.Exists(req.Uuid) {
		log.Println("Recive a Untrusted Node Request")
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.authenticatedConexions.Exists(req.Uuid) {
		log.Println("Recive a not authenticated Node Request")
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "403 forbidden",
		}, nil
	}

	var event Event
	event.Evento_id = req.EventoId
	event.Discoteca = req.Discoteca
	event.Nombre_evento = req.NombreEvento
	event.Categoria = req.Categoria
	event.Comuna = req.Comuna
	event.Precio = int(req.Precio)
	event.Stock = int(req.Stock)
	event.Fecha_evento = req.FechaEvento
	event.Fecha_publicacion = time.Now().Format(time.RFC3339)

	// Validación
	if event.Precio <= 0 || event.Stock <= 0 {
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "Invalid event data",
		}, nil
	}

	// Publish event to DynamoDB
	// TODO: Check is evento_id is unique.
	log.Printf("Publishing Event %s to DynamoDB", event.Evento_id)

	return &pbProducer.PublishEventResponse{
		Success: true,
		Message: "Event published successfully",
	}, nil
}

func (s *ConsumerServer) GetEvents(ctx context.Context, req *pbConsumer.GetEventsRequest) (*pbConsumer.GetEventsResponse, error) {
	if !s.broker.trustedConexions.Exists(req.Uuid) {
		log.Println("Recive a Untrusted Node Request")
		return &pbConsumer.GetEventsResponse{
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.authenticatedConexions.Exists(req.Uuid) {
		log.Println("Recive a not authenticated Node Request")
		return &pbConsumer.GetEventsResponse{
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	// Get events from DynamoDB
	log.Print("Geting all events from DynamoDB :(")

	return &pbConsumer.GetEventsResponse{
		Succes:  true,
		Message: "Events retrieved successfully",
		Events: []*pbConsumer.Events{
			{
				EventID:      "1",
				Discoteca:    "Discoteca A",
				NombreEvento: "Evento A",
				Categoria:    "Categoria A",
				Comuna:       "Comuna A",
				Precio:       10000,
				Stock:        100,
				FechaEvento:  time.Now().Format(time.RFC3339),
			},
		},
	}, nil
}

func (s *ConsumerServer) PurchaseEvent(ctx context.Context, req *pbConsumer.PurchaseEventRequest) (*pbConsumer.PurchaseEventResponse, error) {
	if !s.broker.trustedConexions.Exists(req.Uuid) {
		log.Println("Recive a Untrusted Node Request")
		return &pbConsumer.PurchaseEventResponse{
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	if !s.broker.authenticatedConexions.Exists(req.Uuid) {
		log.Println("Recive a not authenticated Node Request")
		return &pbConsumer.PurchaseEventResponse{
			Succes:  false,
			Message: "403 forbidden",
		}, nil
	}

	// TODO: Get Event from DynamoDB and check if it exists and if there is stock available.

	log.Printf("Processing payment for Event %s", req.EventID)
	success, response, err := s.broker.bancoUSM.ProcessPayment(
		req.Uuid,
		5000, // TODO: Get price from event
		req.PaymentMethod,
	)

	if err != nil {
		log.Printf("Error processing payment: %v", err)
		return &pbConsumer.PurchaseEventResponse{
			Succes:  false,
			Message: "Payment processing failed",
		}, nil
	}

	if !success {
		log.Printf("Payment failed: %s", response)
		return &pbConsumer.PurchaseEventResponse{
			Succes:  false,
			Message: "Payment failed: " + response,
		}, nil
	}

	// TODO: Add unique ticket id
	ticket := "unique"

	log.Printf("Payment successful for Event %s", req.EventID)
	return &pbConsumer.PurchaseEventResponse{
		Succes:  true,
		Message: "Purchase successful",
		TicketID:   ticket,
	}, nil
}

func TicketsGenerator() {

}

func DataStorage() {

}

func DataRestore() {

}

func StartServers(broker *Broker) {
	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pbRegisterAuth.RegisterRegisterAuthServer(grpcServer, &RegisterAuthServer{
		broker: broker,
	})
	pbProducer.RegisterProducerServer(grpcServer, &ProducerServer{
		broker: broker,
	})
	pbConsumer.RegisterConsumerServer(grpcServer, &ConsumerServer{
		broker: broker,
	})

	log.Printf("Servers is listening on %s", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *Broker) InitTables() {
	for !s.dynamoDBClient.CreateTable("Eventos") {
		log.Println("Failed to create table, retrying in 2 seconds...")
		time.Sleep(2 * time.Second)
	}

	for !s.dynamoDBClient.CreateTable("Tickets") {
		log.Println("Failed to create table, retrying in 2 seconds...")
		time.Sleep(2 * time.Second)
	}
}

func serverBackground() {
	broker := &Broker{
		trustedConexions:       dsRegisterMap.NewRegisterMap(),
		authenticatedConexions: dsRegisterMap.NewRegisterMap(),

		bancoUSM:       new(dsConextionManager.BancoUSM),
		dynamoDBClient: new(dsConextionManager.DynamoDB),
	}

	broker.dynamoDBClient.N = 3
	broker.dynamoDBClient.R = 2
	broker.dynamoDBClient.W = 2
	broker.dynamoDBClient.Nodes = make(map[string]dsConextionManager.Node)

	go StartServers(broker)
	time.Sleep(2 * time.Second)

	broker.InitTables()

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
