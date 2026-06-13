package main

import (
	"context"
	"sync"
	"time"
	"log"
	"net"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	DBManager "Broker/DBManager"
	BancoManager "Broker/BancoManager"

	dsUUID "Broker/dsUUID"
	dsRegisterMap "Broker/dsRegisterMap"

	pbBroker "Broker/proto/pbBroker"
	pbDynamoDB "Broker/proto/pbDynamoDB"
	pbProducer "Broker/proto/pbProducer"
	pbConsumer "Broker/proto/pbConsumer"
)

type Broker struct {
	mu sync.Mutex
	DB *DBManager.DBManager
	BANCO *BancoManager.BancoUSM

	TruestedProducers *dsRegisterMap.RegisterMap
}

type BrokerServer struct {
	Broker *Broker
	pbBroker.UnimplementedBrokerServer
}

type ProducerServer struct {
	Broker *Broker
	pbProducer.UnimplementedProducerServer
}

type ConsumerServer struct {
	pbConsumer.UnimplementedConsumerServer
	Broker *Broker
}

func startServer(broker *Broker) {
	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pbBroker.RegisterBrokerServer(grpcServer, &BrokerServer{
		Broker: broker,
	})
	pbProducer.RegisterProducerServer(grpcServer, &ProducerServer{
		Broker: broker,
	})
	pbConsumer.RegisterConsumerServer(grpcServer, &ConsumerServer{
		Broker: broker,
	})

	log.Printf("Servers is listening on %s", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *BrokerServer) CheckIsAlive(ctx context.Context, req *pbBroker.Empty) (*pbBroker.IsAliveResponde, error) {
	return &pbBroker.IsAliveResponde{
		IsAlive: true,
	}, nil
}

func (s *Broker) AddPublisher(IP string, UUID *string) (bool, *string) {
	if UUID != nil {
		if !s.TruestedProducers.Exists(*UUID) {
			log.Printf("[BROKER] Recive a untrusted Producer (%s)", IP)
			return false, nil
		} else {
			log.Panicf("[BROKER] Got a Producer Back (%s)", IP)
			return true, UUID
		}
	}

	var newUUID = dsUUID.NewUUID().String()
	s.TruestedProducers.Add(newUUID, IP)

	log.Printf("[BROKER] Added a new Producer (%s)", IP)

	return true, &newUUID
}

func (s *BrokerServer) Handshake(ctx context.Context, req *pbBroker.HandshakeRequest) (*pbBroker.HandshakeResponse, error) {
	s.Broker.mu.Lock()
	defer s.Broker.mu.Unlock()

	IP := req.Direction
	log.Printf("Got Handshake Request from: %s", IP)

	var success = false
	var key *string
	switch req.WhatIAm {
	case "NODE":
		success = s.Broker.DB.AddConection(IP)
	case "BANCO":
		success = s.Broker.BANCO.Connect(IP)
	case "PRODUCTOR":
		success, key = s.Broker.AddPublisher(IP, req.Uuid)
	case "CONSUMIDOR":
	
	}

	if !success {
		log.Printf("Handshake rejected or fail: %s", req.WhatIAm)
		return &pbBroker.HandshakeResponse{
			Success: false,
			UUID: key,
		}, nil
	}

	log.Printf("Success Conecting to Node at: %s", IP)
	return &pbBroker.HandshakeResponse{
		Success: true,
		UUID: key,
	}, nil
}

func (s *ProducerServer) PublishEvent(ctx context.Context, req *pbProducer.PublishEventRequest) (*pbProducer.PublishEventResponse, error) {
	if !s.Broker.TruestedProducers.Exists(req.Uuid) {
		log.Println("[BROKER-PROD] Recive a Untrusted Node Request")
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "403 forbidden",
		}, nil
	}

	var Event pbBroker.Event
	Event.EventID 			= req.EventoId
	Event.Discoteca 		= req.Discoteca
	Event.NombreEvento 		= req.NombreEvento
	Event.Categoria 		= req.Categoria
	Event.Comuna 			= req.Comuna
	Event.Precio 			= int32(req.Precio)
	Event.Stock 			= int32(req.Stock)
	Event.SpendStock		= int32(0)
	Event.FechaEvento 		= req.FechaEvento
	Event.FechaPublicacion 	= time.Now().Format(time.RFC3339)

	// Validación
	if Event.Precio <= 0 || Event.Stock <= 0 {
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "Invalid event data",
		}, nil
	}

	// Publish event to DynamoDB
	item, success := s.Broker.DB.GetItem("Eventos", Event.EventID)
	if success == false {
		log.Printf("[BROKER-PROD] Could not verify if exists: %s", Event.EventID)
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "Could not verify if exists",
		}, nil
	}

	if item.Exists {
		log.Printf("[BROKER-PROD] Event Already Publish: %s", Event.EventID)
		return &pbProducer.PublishEventResponse{
			Success: true,
			Message: "Event Already Publish",
		}, nil
	}

	sendData, err := proto.Marshal(&Event)
	if err != nil {
		log.Printf("[BROKER-PROD] Unable to Marshal Event, err: %v", err)
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "Unable to Marshal Event",
		}, err
	}

	success = s.Broker.DB.PutItem("Eventos", Event.EventID, sendData)
	if (!success) {
		log.Printf("[BROKER-PROD] Fail to Store Event %s", Event.EventID)
		return &pbProducer.PublishEventResponse{
			Success: false,
			Message: "Fail to store event ",
		}, nil
	}

	log.Printf("[BROKER-PROD] Event %s Published Successfully", Event.EventID)
	return &pbProducer.PublishEventResponse{
		Success: true,
		Message: "Event Published Successfully",
	}, nil
}

func (s *Broker) InitTables() {
	for !s.DB.CreateTable("Eventos") {
		log.Println("Failed to create table, retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
	}

	for !s.DB.CreateTable("Tickets") {
		log.Println("Failed to create table, retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
}

func (s *Broker) TestDB() {
	for i := 0; i < 10; i++ {
		data := pbBroker.Data{
			Data: int32(i),
		}

		item, err := proto.Marshal(&data)
		if err != nil {
			log.Panicln("What")
		}

		for !s.DB.PutItem("Eventos", fmt.Sprintf("Key-%d", i), item) {
			log.Println("Failed to put item, retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
		}
	}

	for i := 0; i < 10; i++ {
		var item *pbDynamoDB.Item
		success := false
		for !success {
			item, success = s.DB.GetItem("Eventos", fmt.Sprintf("Key-%d", i))
			time.Sleep(5 * time.Second)
		}

		var value pbBroker.Data
		proto.Unmarshal(item.Value, &value)

		if int(value.Data) != i {
			log.Println("Is not the same")
			i--
			continue
		}
		time.Sleep(10 * time.Second)
	}
}

func serverBackground() {
	broker := &Broker{
		DB: DBManager.CreateDBManager(3, 2, 2),
		BANCO: BancoManager.CreateBancoManager(),

		TruestedProducers: dsRegisterMap.NewRegisterMap(),
	}

	go startServer(broker)

	broker.InitTables()

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
