package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"
	"math/rand"

	"encoding/json"

	"google.golang.org/grpc"

	pbProducer "Productor/proto/pbProducer"
	pbRegisterAuth "Productor/proto/pbRegisterAuth"
)

type Producer struct {
	BrokerConnection *grpc.ClientConn
	RegisterAuthClient pbRegisterAuth.RegisterAuthClient
	ProducerClient pbProducer.ProducerClient

	KeyPath string
	InputPath string

	eventosProgramados map[string]Event
	eventosPublicados  map[string]Event

	Uuid string
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

func (c *Producer) Register(username string) (string, bool) {
	message := pbRegisterAuth.RegisterRequest{
		Username: username,
		Type:     "producer",
	}

	response, err := c.RegisterAuthClient.Register(context.Background(), &message)
	if err != nil {
		log.Printf("Error registering producer: %v", err)
		return "", false
	}

	if !response.Success {
		log.Printf("Failed to register producer %s: %s", username, response.Message)
		return "", false
	}

	log.Printf("producer %s registered successfully: UUID=%s", username, response.Uuid)
	return response.Uuid, true
}

func (c *Producer) Authenciate(username string, uuid string) bool {
	message := pbRegisterAuth.AuthRequest{
		Username: username,
		Uuid: uuid,
	}

	response, err := c.RegisterAuthClient.Authenticate(context.Background(), &message)
	if err != nil {
		log.Printf("Error authenticating consumer: %v", err)
		return false
	}

	log.Println(response.Message)
	return response.Success
}

func (c *Producer) LoginSession() bool {
	key, succes := c.ReadUUID()
	if succes {
		succes = c.Authenciate(os.Getenv("NAME"), key)
	}

	if !succes {
		for {
			key, succes = c.Register(os.Getenv("NAME"))
			if succes {
				break;
			}
			log.Println("Retrining in 5 second...")
			time.Sleep(5 * time.Second)
		}
		c.SaveUUID(key)
	} else {
		return true
	}

	c.Uuid = key
	return c.Authenciate(os.Getenv("NAME"), key)
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
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
		c.eventosProgramados[item.Evento_id] = item
	}

	// Read ']'
	_, err = decoder.Token()
	if err != nil {
		log.Fatalf("Failed to read json: %v", err)
	}
}

func (c *Producer) PublishEvent(eventId string) bool {
	event, exists := c.eventosProgramados[eventId]
	if !exists {
		log.Printf("Event with ID %s not found", eventId)
		return false
	}

	res, err := c.ProducerClient.PublishEvent(context.Background(), &pbProducer.PublishEventRequest{
		Uuid: c.Uuid,
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

	c.eventosPublicados[eventId] = event
	delete(c.eventosProgramados, eventId)

	log.Printf("Published Event: %+v", event)
	return true
}

func (c *Producer) PublishAllEvents() {
	for {
		for key := range c.eventosProgramados {
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

func initProducer() {
	BrokerConnection := NewGRPCClient(os.Getenv("BROKER_URL"))
	defer BrokerConnection.Close()

	producer := &Producer{
		BrokerConnection: BrokerConnection,
		RegisterAuthClient: pbRegisterAuth.NewRegisterAuthClient(BrokerConnection),
		ProducerClient: pbProducer.NewProducerClient(BrokerConnection),
		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
		InputPath: "input/" + os.Getenv("INPUT_FILE"),

		eventosProgramados: make(map[string]Event),
		eventosPublicados:  make(map[string]Event),
	}

	producer.LoginSession()
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
