package main

import (
	"os"
	"log"
	"time"
	"bufio"
	"context"
	"math/rand"

	"encoding/csv"

	"google.golang.org/grpc"

	pbBroker "Consumer/proto/pbBroker"
	pbConsumer "Consumer/proto/pbConsumer"
)

type Consumer struct {
	Broker pbBroker.BrokerClient
	ConsumerClient pbConsumer.ConsumerClient

	KeyPath string
	OutputPath string
	
	EventosDisponibles []Event

	UUID string
}

func (c *Consumer) SaveUUID(uuid string) {
	file, err := os.Create(c.KeyPath)
	if err != nil {
		log.Fatalf("Error Creating file: %v", err)
	}
	defer file.Close()

	file.WriteString(uuid + "\n")
	log.Println("Key Saved, Now you can safely close your sesion")
}

func (c *Consumer) ReadUUID() (string, bool) {
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

func (c *Consumer) Handshake() bool {
	conn := NewGRPCClient(os.Getenv("BROKER_URL"))
	c.Broker = pbBroker.NewBrokerClient(conn)
	c.ConsumerClient = pbConsumer.NewConsumerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	req := &pbBroker.HandshakeRequest{
		Direction: os.Getenv("NAME"),
		WhatIAm: "CONSUMIDOR",
	}

	key, success := c.ReadUUID()
	if success {
		req.Uuid = &key
	}

	res, err := c.Broker.Handshake(ctx, req)
	if err != nil {
		log.Printf("Faild to do Handshake: %v", err)
		return false
	}

	if !res.Success {
		req.Uuid = nil
		
		log.Printf("[CONS] Getting a new key")
		res, err = c.Broker.Handshake(ctx, req)
		if err != nil {
			log.Printf("Faild to do Handshake: %v", err)
			return false
		}
	}

	_, err = c.Broker.CheckIsAlive(context.Background(), &pbBroker.Empty{})
	if err != nil {
		log.Printf("Faild to check Alive: %v", err)
		return false
	}

	c.UUID = *res.UUID
	c.SaveUUID(*res.UUID)

	if *res.UUID == key {
		log.Println("[CONS] Connexion Restablesh")
		return true
	}

	log.Print("[CONS] Connexion Succsess")
	return true
}

func (c *Consumer) CreateCSV() {
	file, err := os.Create(c.OutputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	rows := [][]string{
		{"tickerID", "eventID", "paymentMethod", "eventDate", "purchaseDate"},
	}

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}
	}

	if err := writer.Error(); err != nil {
		log.Fatal(err)
	}
}

func (c *Consumer) WriteOutTicket(recipe *pbConsumer.PurchaseEntry) {
	file, err := os.OpenFile(
		c.OutputPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write([]string{recipe.TicketID, recipe.EventID, recipe.PaymentMethod, recipe.FechaEvento, recipe.FechaCompra})
	if err != nil {
		log.Fatal(err)
	}
}

type Event struct {
	EventID string
	Discoteca string
	NombreEvento string
	Categoria string
	Comuna string
	Precio int
	Stock int
	FechaEvento string
	FechaPublicacion string
}

func (c *Consumer) GetEvents() {
	response, err := c.ConsumerClient.GetEvents(context.Background(), &pbConsumer.GetEventsRequest{
		Uuid: c.UUID,
	})
	if err != nil {
		log.Printf("[CONS] Error getting events: %v", err)
		return
	}

	if !response.Success {
		log.Printf("[CONS] Failed to get events: %s", response.Message)
		return
	}

	c.EventosDisponibles = make([]Event, 0)

	log.Printf("[CONS] Received %d events", len(response.Events))
	for _, event := range response.Events {
		var newEvent Event
		newEvent.EventID = event.EventID
		newEvent.Discoteca = event.Discoteca
		newEvent.NombreEvento = event.NombreEvento
		newEvent.Categoria = event.Categoria
		newEvent.Comuna = event.Comuna
		newEvent.Precio = int(event.Precio)
		newEvent.Stock = int(event.Stock)
		newEvent.FechaEvento = event.FechaEvento
		newEvent.FechaPublicacion = event.FechaPublicacion

		c.EventosDisponibles = append(c.EventosDisponibles, newEvent)
	}
}

func (c *Consumer) BuyEvent(idx int) {
	var payMethod string
	if rand.Float32() <= 0.5 {
		payMethod = "debito"
	} else {
		payMethod = "credito"
	}

	res, err := c.ConsumerClient.PurchaseEvent(context.Background(), &pbConsumer.PurchaseEventRequest{
		Uuid: c.UUID,
		EventID: c.EventosDisponibles[idx].EventID,
		PaymentMethod: payMethod,
		Quantity: 1,
	})
	if err != nil {
		log.Printf("[CONS] Server respond with error: %v", err)
		return
	}

	if !res.Success {
		log.Printf("[CONS] Purchase Fail: %s", res.Message)
		return
	}

	log.Printf("[CONS] Purchase Success")
	log.Printf("[CONS] %+v", res.PurchaseResult)
	c.WriteOutTicket(res.PurchaseResult)
}	

func (c *Consumer) Simulation() {
	c.CreateCSV()

	for {
		c.GetEvents()

		if (len(c.EventosDisponibles) > 0) {
			randomEvent := rand.Intn(len(c.EventosDisponibles))
			c.BuyEvent(randomEvent)
		}

		time.Sleep(time.Duration(5.0+15.0*rand.Float32()) * time.Second)
	}
}

func initConsumer() {
	consumer := &Consumer{
		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
		OutputPath: "output/" + os.Getenv("NAME") + ".csv",

		EventosDisponibles: make([]Event, 0),
	}

	for !consumer.Handshake() {
		time.Sleep(2 * time.Second)
	}

	go consumer.Simulation()

	forever := make(chan bool)
	log.Printf("Simulating. To exit press CTRL+C")
	<-forever
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	log.Printf("%s is running on port %s", name, port)

	initConsumer()
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
}
