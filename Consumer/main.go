package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"

	pbBroker "Consumer/proto/pbBroker"
	pbConsumer "Consumer/proto/pbConsumer"
)

type Consumer struct {
	Broker pbBroker.BrokerClient
	ConsumerClient pbConsumer.ConsumerClient

	KeyPath string
	OutputPath string
	
	EventosDisponibles map[string]Event

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

type Event struct {
	Evento_id string
	Discoteca string
	Nombre_evento string
	Categoria string
	Comuna string
	Precio int
	Stock int
	Fecha_evento string
	Fecha_publicacion string
}

func (c *Consumer) GetEvents() {
	response, err := c.ConsumerClient.GetEvents(context.Background(), &pbConsumer.GetEventsRequest{
		Uuid: c.UUID,
	})
	if err != nil {
		log.Printf("Error getting events: %v", err)
		return
	}

	if !response.Succes {
		log.Printf("Failed to get events: %s", response.Message)
		return
	}

	log.Printf("Received %d events", len(response.Events))
	for _, event := range response.Events {
		log.Printf("Event ID: %s, Name: %s, Price: %d", event.EventID, event.NombreEvento, event.Precio)
	}

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

func initConsumer() {
	consumer := &Consumer{
		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
		OutputPath: "output/" + os.Getenv("NAME") + ".csv",

		EventosDisponibles: make(map[string]Event),
	}

	for !consumer.Handshake() {
		time.Sleep(2 * time.Second)
	}
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
