package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"

	pbRegisterAuth "Consumidor/proto/pbRegisterAuth"
	pbConsumer "Consumidor/proto/pbConsumer"
)

type Consumer struct {
	BrokerConnection *grpc.ClientConn
	RegisterAuthClient pbRegisterAuth.RegisterAuthClient
	ConsumerClient pbConsumer.ConsumerClient

	KeyPath string
	Uuid string

	eventosDisponibles map[string]Event
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

func (c *Consumer) Register(username string) (string, bool) {
	message := pbRegisterAuth.RegisterRequest{
		Username: username,
		Type:     "consumer",
	}

	response, err := c.RegisterAuthClient.Register(context.Background(), &message)
	if err != nil {
		log.Printf("Error registering consumer: %v", err)
		return "", false
	}

	if !response.Success {
		log.Printf("Failed to register consumer %s: %s", username, response.Message)
		return "", false
	}

	log.Printf("Consumer %s registered successfully: UUID=%s", username, response.Uuid)
	return response.Uuid, true
}

func (c *Consumer) Authenciate(username string, uuid string) bool {
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

func (c *Consumer) LoginSession() bool {
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

func (c *Consumer) GetEvents() {
	response, err := c.ConsumerClient.GetEvents(context.Background(), &pbConsumer.GetEventsRequest{
		Uuid: c.Uuid,
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

func initConsumer() {
	BrokerConnection := NewGRPCClient(os.Getenv("BROKER_URL"))
	defer BrokerConnection.Close()

	consumer := &Consumer{
		BrokerConnection: BrokerConnection,
		RegisterAuthClient: pbRegisterAuth.NewRegisterAuthClient(BrokerConnection),
		ConsumerClient: pbConsumer.NewConsumerClient(BrokerConnection),
		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
	}

	consumer.LoginSession()

}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	log.Printf("%s is running on port %s", name, port)

	initConsumer()
}
