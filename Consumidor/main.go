package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"

	pbRegisterAuth "Consumidor/proto/pbRegisterAuth"
)

type Consumer struct {
	BrokerConnection *grpc.ClientConn
	RegisterAuthClient pbRegisterAuth.RegisterAuthClient

	KeyPath string
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

	fmt.Println(uuid)
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

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
}

func initConsumer() {
	BrokerConnection := NewGRPCClient(os.Getenv("BROKER_URL"))
	defer BrokerConnection.Close()

	consumer := &Consumer{
		BrokerConnection: BrokerConnection,
		RegisterAuthClient: pbRegisterAuth.NewRegisterAuthClient(BrokerConnection),
		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
	}

	key, succes := consumer.Register(os.Getenv("NAME"))
	if !succes {
		log.Fatalln("Unable to Register to Broker :(")
	}

	consumer.SaveUUID(key)
	key, succes = consumer.ReadUUID()
	log.Println(key)
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	log.Printf("%s is running on port %s", name, port)

	initConsumer()
}
