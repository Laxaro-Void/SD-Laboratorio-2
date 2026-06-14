package main

import (
	"context"
	"sync"
	"time"
	"log"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	DBManager "Broker/DBManager"
	BancoManager "Broker/BancoManager"

	dsUUID "Broker/dsUUID"
	dsRegisterMap "Broker/dsRegisterMap"

	pbBroker "Broker/proto/pbBroker"
	pbProducer "Broker/proto/pbProducer"
	pbConsumer "Broker/proto/pbConsumer"
)

type Broker struct {
	mu sync.Mutex
	DB *DBManager.DBManager
	BANCO *BancoManager.BancoUSM

	TruestedProducers *dsRegisterMap.RegisterMap
	TrustedConsumer   *dsRegisterMap.RegisterMap

	// Reporte
	// Discotecas
	D_TotalEnviados map[string]int
	D_TotalAceptados int
	D_TotalRechazados int
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

// Inicializar los Servidores del Broker
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

// Funcion para Ve si esta vivo el broker
func (s *BrokerServer) CheckIsAlive(ctx context.Context, req *pbBroker.Empty) (*pbBroker.IsAliveResponde, error) {
	return &pbBroker.IsAliveResponde{
		IsAlive: true,
	}, nil
}

// Agrega un Publisher/Discoteca
func (s *Broker) AddPublisher(IP string, UUID *string) (bool, *string) {
	if UUID != nil {
		if !s.TruestedProducers.Exists(*UUID) {
			log.Printf("[BROKER] Recive a untrusted Producer (%s)", IP)
			return false, nil
		} else {
			log.Printf("[BROKER] Got a Producer Back (%s)", IP)
			return true, UUID
		}
	}

	var newUUID = dsUUID.NewUUID().String()
	s.TruestedProducers.Add(newUUID, IP)

	log.Printf("[BROKER] Added a new Producer (%s)", IP)

	return true, &newUUID
}

// Agrega un Consumer/Usuario
func (s *Broker) AddConsumer(IP string, UUID *string) (bool, *string) {
	if UUID != nil {
		if !s.TrustedConsumer.Exists(*UUID) {
			log.Printf("[BROKER] Recive a untrusted Consumer (%s)", IP)
			return false, nil
		} else {
			log.Printf("[BROKER] Got a Consumer Back (%s)", IP)
			return true, UUID
		}
	}

	var newUUID = dsUUID.NewUUID().String()
	s.TrustedConsumer.Add(newUUID, IP)

	log.Printf("[BROKER] Added a new Consumer (%s)", IP)

	return true, &newUUID
}

// Funcion que permite el registro de los nodos. Sin realizar esto, los mensajes entrantes son rechazados
func (s *BrokerServer) Handshake(ctx context.Context, req *pbBroker.HandshakeRequest) (*pbBroker.HandshakeResponse, error) {
	s.Broker.mu.Lock()
	defer s.Broker.mu.Unlock()

	IP := req.Direction
	log.Printf("[BROKER] Got Handshake Request from: %s", IP)

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
		success, key = s.Broker.AddConsumer(IP, req.Uuid)
	}

	if !success {
		log.Printf("[BROKER] Handshake rejected or fail: %s", req.WhatIAm)
		return &pbBroker.HandshakeResponse{
			Success: false,
			UUID: key,
		}, nil
	}

	log.Printf("[BROKER] Success Conecting to Node at: %s", IP)
	return &pbBroker.HandshakeResponse{
		Success: true,
		UUID: key,
	}, nil
}

// ProducerServer - Publica un evento a la BD
func (s *ProducerServer) PublishEvent(ctx context.Context, req *pbProducer.PublishEventRequest) (*pbProducer.PublishEventResponse, error) {
	s.Broker.mu.Lock()
	defer s.Broker.mu.Unlock()

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
	Event.FechaPublicacion 	= time.Now().Format("2006-01-02T15:04:05")

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

// ConsumerServer - Obtiene Eventos para el Consumidor
func (s *ConsumerServer) GetEvents(ctx context.Context, req *pbConsumer.GetEventsRequest) (*pbConsumer.GetEventsResponse, error) {
	s.Broker.mu.Lock()
	defer s.Broker.mu.Unlock()

	if !s.Broker.TrustedConsumer.Exists(req.Uuid) {
		log.Println("[BROKER-CONS] Recive a Untrusted Node Request")
		return &pbConsumer.GetEventsResponse{
			Success: false,
			Message: "403 forbidden",
		}, nil
	}

	table, success := s.Broker.DB.GetTable("Eventos")
	if !success {
		log.Printf("[BROKER-CONS] Fail to get Table %s", "Eventos")
		return &pbConsumer.GetEventsResponse{
			Success: false,
			Message: "Fail to get Table Eventos",
			Events: nil,
		}, nil
	}

	log.Printf("[BROKER-CONS] Got Table %s", "Eventos")
	var Eventos []*pbConsumer.Event
	for _, dataByte := range table.Value {
		var eventoBroker pbBroker.Event
		var eventoConsumer pbConsumer.Event
		err := proto.Unmarshal(dataByte, &eventoBroker)
		if err != nil {
			log.Panic("[BROKER-CONS] Faild to Interpret Byte to Event")
		}

		eventoConsumer.EventID = eventoBroker.EventID
		eventoConsumer.Discoteca = eventoBroker.Discoteca
		eventoConsumer.NombreEvento = eventoBroker.NombreEvento
		eventoConsumer.Categoria = eventoBroker.Categoria
		eventoConsumer.Comuna = eventoBroker.Comuna
		eventoConsumer.Precio = eventoBroker.Precio
		eventoConsumer.Stock = eventoBroker.Stock
		eventoConsumer.SpendStock = eventoBroker.SpendStock
		eventoConsumer.FechaEvento = eventoBroker.FechaEvento
		eventoConsumer.FechaPublicacion = eventoBroker.FechaPublicacion
		
		Eventos = append(Eventos, &eventoConsumer)
	}

	return &pbConsumer.GetEventsResponse{
		Success: true,
		Message: "Got Table Eventos",
		Events: Eventos,
	}, nil
}

// ConsumerServer - Servicio de Compra de Eventos Se comunica con la BD y el Banco
func (s *ConsumerServer) PurchaseEvent(ctx context.Context, req *pbConsumer.PurchaseEventRequest) (*pbConsumer.PurchaseEventResponse, error) {
	s.Broker.mu.Lock()
	defer s.Broker.mu.Unlock()

	if !s.Broker.TrustedConsumer.Exists(req.Uuid) {
		log.Println("[BROKER-CONS] Recive a Untrusted Node Request")
		return &pbConsumer.PurchaseEventResponse{
			Success: false,
			Message: "403 forbidden",
		}, nil
	}

	// Obtiene El evento
	item, success := s.Broker.DB.GetItem("Eventos", req.EventID)
	if success == false {
		log.Printf("[BROKER-CONS] Could not verify if exists: %s", req.EventID)
		return &pbConsumer.PurchaseEventResponse{
			Success: false,
			Message: "Could not verify if exists",
		}, nil
	}

	// Existencia
	if !item.Exists {
		log.Printf("[BROKER-CONS] Event Do Not Exists: %s", req.EventID)
		return &pbConsumer.PurchaseEventResponse{
			Success: true,
			Message: "Event Do Not Exists",
		}, nil
	}

	var Event pbBroker.Event
	err := proto.Unmarshal(item.Value, &Event)
	if err != nil {
		log.Printf("[BROKER-CONS] Unable to Marshal Event, err: %v", err)
		return &pbConsumer.PurchaseEventResponse{
			Success: false,
			Message: "Unable to Marshal Event",
		}, err
	}

	// Valida si hay stock
	if Event.SpendStock >= Event.Stock {
		log.Printf("[BROKER-CONS] Event %s is sold out", req.EventID)
		return &pbConsumer.PurchaseEventResponse{
			Success: false,
			Message: "Sold out",
		}, nil
	}

	// Servicio de Banco
	success, message := s.Broker.BANCO.ProcessPayment(req.Uuid, Event.Precio, req.PaymentMethod)
	if !success {
		log.Printf("[BROKER-CONS] %s", message)
		return &pbConsumer.PurchaseEventResponse{
			Success: false,
			Message: message,
		}, nil
	}

	// Siempre unico
	ticketId := req.EventID + "-T" + strconv.FormatInt(int64(Event.SpendStock), 10)

	// Prepara para subir la compra a la BD
	var Purchase pbConsumer.PurchaseEntry
	Purchase.UUID = req.Uuid
	Purchase.EventID = req.EventID
	Purchase.TicketID = ticketId
	Purchase.PaymentMethod = req.PaymentMethod
	Purchase.FechaEvento = Event.FechaEvento
	Purchase.FechaCompra = time.Now().Format("2006-01-02T15:04:05")

	sendData, err := proto.Marshal(&Purchase)
	if err != nil {
		log.Printf("[BROKER-CONS] Fail to Marshal Purchase: %v", err)
		return &pbConsumer.PurchaseEventResponse{
			Success: false,
			Message: "Fail to Marshal Purchase",
		}, err
	}
	
	// Fuerza que la publicacion ocurra
	for !s.Broker.DB.PutItem("Tickets", Purchase.TicketID, sendData) {
		log.Printf("[BROKER-CONS] Retry push Purchase")
		time.Sleep(5 * time.Second)
	}

	// Actualiza el evento en la BD
	Event.SpendStock = Event.SpendStock + 1
	sendData, err = proto.Marshal(&Event)

	for !s.Broker.DB.PutItem("Eventos", Event.EventID, sendData) {
		log.Printf("[BROKER-CONS] Retry push Event Update")
		time.Sleep(5 * time.Second)
	}

	return &pbConsumer.PurchaseEventResponse{
		Success: true,
		Message: "Purchase Success",
		PurchaseResult: &Purchase,
	}, nil
}

// Inicializa las tablas en la BD
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

func serverBackground() {
	broker := &Broker{
		DB:    DBManager.CreateDBManager(3, 2, 2), // Gestor de la BD
		BANCO: BancoManager.CreateBancoManager(),  // Gesto del Banco

		TruestedProducers: dsRegisterMap.NewRegisterMap(), // Registro de Conexiones
		TrustedConsumer:   dsRegisterMap.NewRegisterMap(), // Registro de Conexiones
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
