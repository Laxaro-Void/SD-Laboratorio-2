package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"encoding/json"

	"google.golang.org/grpc"

	pbRegisterAuth "Productor/proto/pbRegisterAuth"
)

type Producer struct {
	BrokerConnection *grpc.ClientConn
	RegisterAuthClient pbRegisterAuth.RegisterAuthClient

	KeyPath string
	InputPath string

	eventosProgramados map[string]Event
	eventosPublicados  map[string]Event
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
	evento_id string
	discoteca string
	nombre_evento string
	categoria string
	comuna string
	precio int
	stock int
	fecha_evento time.Time
	fecha_publicacion time.Time
}

func (c *Producer) ReadInput() {
	file, err := os.Open(c.InputPath)
	if err != nil {
		log.Fatalf("Faild to open json: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var item Event
		if err := decoder.Decode(&item); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Faild to decode item: %v", err)
		}

		if item.discoteca != os.Getenv("NAME") {
			continue
		}
		log.Printf("Read Event: %+v", item)
		c.eventosProgramados[item.evento_id] = item
	}
}

func initProducer() {
	BrokerConnection := NewGRPCClient(os.Getenv("BROKER_URL"))
	defer BrokerConnection.Close()

	producer := &Producer{
		BrokerConnection: BrokerConnection,
		RegisterAuthClient: pbRegisterAuth.NewRegisterAuthClient(BrokerConnection),

		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
		InputPath: "input/" + os.Getenv("INPUT_FILE"),

		eventosProgramados: make(map[string]Event),
		eventosPublicados:  make(map[string]Event),
	}

	producer.LoginSession()
	producer.ReadInput()
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	log.Printf("%s is running on port %s", name, port)

	initProducer()
}
