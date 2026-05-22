package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"context"
	"strconv"

	pbServer "client/proto/server"

	"github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

func dialRabbitMQ() (*amqp091.Connection, error) {
	var maxAttempts = 10
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		log.Printf("Attempting to connect to RabbitMQ (Attempt %d/%d)", attempt, maxAttempts)
		conn, err := amqp091.Dial(os.Getenv("RABBITMQ-URL"))
		if err == nil {
			log.Printf("Successfully connected to RabbitMQ on attempt %d", attempt)
			return conn, nil
		}
		log.Printf("Failed to connect to RabbitMQ on attempt %d: %v", attempt, err)
		time.Sleep(5 * time.Second)
	}
	return nil, fmt.Errorf("could not connect to RabbitMQ after %d attempts", maxAttempts)
}

type Client struct {
	connRabbitMQ *amqp091.Connection
	serverServiceClient pbServer.ServerServiceClient
}

func (c *Client) createTasks() {
	tasks := 10
	for i := 0; i < tasks; i++ {
		taskName := fmt.Sprintf("Task #%d", i+1)

		request := &pbServer.ServerRequest{
			TaskName: taskName,
			TaskData: int32(i),
		}

		log.Printf("Submitting task: %s", taskName)
		res, err := c.serverServiceClient.Submit(context.Background(), request)
		if err != nil {
			log.Printf("Failed to submit task: %v", err)
		} else {
			log.Printf("Server response: %s", res.Status)
		}

		time.Sleep(1 * time.Second) // Simulate some delay between task submissions
	}

	c.wirteOut()
}

func (c *Client) wirteOut() {
	output := make([]byte, 0)
	output = fmt.Appendf(output, "Hello output file from client\n")

	strToInt := strconv.Itoa(67)
	output = fmt.Appendf(output, "This is a string from a int %s\n", strToInt)

	intToStr, err := strconv.Atoi("76")
	if err != nil {
		log.Fatalf("Failed to parse string: %v", err)
	}
	output = fmt.Appendf(output, "This is a int from a string %d\n", intToStr)
	
	os.WriteFile("/app/output/output.txt", output, 0644)
}

func initClient() {
	// RabbitMQ
	connRabbitMQ, err := dialRabbitMQ()
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
	}
	defer connRabbitMQ.Close()

	// gRPC server dial
	connServer, err := grpc.Dial(os.Getenv("SERVER-IP"), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer connServer.Close()
	
	client := &Client{
		connRabbitMQ: connRabbitMQ,
		serverServiceClient: pbServer.NewServerServiceClient(connServer),
	}

	// Create and submit tasks
	client.createTasks()
}

func main() {
	initClient()
}
