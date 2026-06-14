package main

import (
	"os"
	"fmt"
	"log"
	"time"
	"bufio"
	"context"
	"math/rand"

	"encoding/json"

	"google.golang.org/grpc"

	pbProducer "Productor/proto/pbProducer"
	pbBroker "Productor/proto/pbBroker"
)

type Producer struct {
	Broker pbBroker.BrokerClient
	ProducerClient pbProducer.ProducerClient

	KeyPath string
	InputPath string

	EventosProgramados map[string]Event
	EventosPublicados  map[string]Event

	UUID string
}

func (c *Producer) SaveUUID(uuid string) {
	file, err := os.Create(c.KeyPath)
	if err != nil {
		log.Fatalf("Error Creating file: %v", err)
	}
	defer file.Close()

	file.WriteString(uuid + "\n")
	log.Println("Key Saved, Now you can safely close your sesion")
}

func (c *Producer) ReadUUID() (string, bool) {
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

	fmt.Println(uuid)
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

func (c *Producer) ReadInput() {
	file, err := os.Open(c.InputPath)
	if err != nil {
		log.Fatalf("Faild to open json: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	// Read '['
	_, err = decoder.Token()
	if err != nil {
		log.Fatalf("Failed to read json: %v", err)
	}

	for decoder.More() {
		var item Event
		err := decoder.Decode(&item)
		if err != nil {
			log.Fatalf("Failed to decode json: %v", err)
		}
		if item.Discoteca != os.Getenv("NAME") {
			continue
		}
		log.Printf("Read Event: %+v", item)
		c.EventosProgramados[item.Evento_id] = item
	}

	// Read ']'
	_, err = decoder.Token()
	if err != nil {
		log.Fatalf("Failed to read json: %v", err)
	}
}

func (c *Producer) PublishEvent(eventId string) bool {
	event, exists := c.EventosProgramados[eventId]
	if !exists {
		log.Printf("Event with ID %s not found", eventId)
		return false
	}

	res, err := c.ProducerClient.PublishEvent(context.Background(), &pbProducer.PublishEventRequest{
		Uuid: c.UUID,
		EventoId: event.Evento_id,
		Discoteca: event.Discoteca,
		NombreEvento: event.Nombre_evento,
		Categoria: event.Categoria,
		Comuna: event.Comuna,
		Precio: int32(event.Precio),
		Stock: int32(event.Stock),
		FechaEvento: event.Fecha_evento,
		FechaPublicacion: event.Fecha_publicacion,
	})
	if err != nil {
		log.Printf("Failed to publish event %s: %v", eventId, err)
		return false
	}

	if !res.Success {
		log.Printf("Failed to publish event %s: %s", eventId, res.Message)
		return false
	}

	c.EventosPublicados[eventId] = event
	delete(c.EventosProgramados, eventId)

	log.Printf("Published Event: %s - %s", event.Evento_id, event.Nombre_evento)
	return true
}

func (c *Producer) PublishAllEvents() {
	for {
		for key := range c.EventosProgramados {
			if c.PublishEvent(key) {
				log.Printf("Event %s published successfully", key)
			} else {
				log.Printf("Failed to publish event %s", key)
			}
			break
		}

		time.Sleep(time.Duration(30.0+10.0*rand.Float32()) * time.Second)
	}
}

func (c *Producer) Handshake() bool {
	conn := NewGRPCClient(os.Getenv("BROKER_URL"))
	c.Broker = pbBroker.NewBrokerClient(conn)
	c.ProducerClient = pbProducer.NewProducerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	req := &pbBroker.HandshakeRequest{
		Direction: os.Getenv("NAME"),
		WhatIAm: "PRODUCTOR",
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
		
		log.Printf("[PROD] Getting a new key")
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
		log.Println("[PROD] Connexion Restablesh")
		return true
	}

	log.Print("[PROD] Connexion Succsess")
	return true
}

func initProducer() {
	producer := &Producer{
		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
		InputPath: "input/" + os.Getenv("INPUT_FILE"),

		EventosProgramados: make(map[string]Event),
		EventosPublicados:  make(map[string]Event),
	}

	for !producer.Handshake() {
		time.Sleep(2 * time.Second)
	}

	producer.ReadInput()
	go producer.PublishAllEvents()

	forever := make(chan bool)
	log.Printf("Sending events. To exit press CTRL+C")
	<-forever
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	log.Printf("%s is running on port %s", name, port)

	initProducer()
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
}
