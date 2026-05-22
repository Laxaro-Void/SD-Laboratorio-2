package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
	"context"

	pbServer "server/proto/server"
	pbWorker "server/proto/worker"

	"google.golang.org/protobuf/proto"
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

type Server struct {
	pbServer.UnimplementedServerServiceServer
	connRabbitMQ *amqp091.Connection
	listener      net.Listener

}

func (s *Server) Submit(ctx context.Context, req *pbServer.ServerRequest) (*pbServer.ServerResponse, error) {
	// Open a channel to RabbitMQ
	channel, err := s.connRabbitMQ.Channel()
	if err != nil {
		log.Printf("Failed to open RabbitMQ channel: %v", err)
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
	}
	defer channel.Close()

	// Take the request data and marshal it into a protobuf message for the worker
	body, err := proto.Marshal(&pbWorker.WorkerRequest{
		WorkCount: int32(0),
		Data: req.TaskData,
	})
	if err != nil {
		log.Printf("Failed to marshal request: %v", err)
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Publish the message to RabbitMQ
	err = channel.Publish(
		"",                // exchange
		"task_queue",      // routing key
		false,             // mandatory
		false,             // immediate
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	if err != nil {
		log.Printf("Failed to publish message to RabbitMQ: %v", err)
		return nil, fmt.Errorf("failed to publish message to RabbitMQ: %v", err)
	}

	// Return a response to the client
	return &pbServer.ServerResponse{Status: "Task submitted successfully"}, nil
}

func initServer() {
	// Initialize server components
	// RabbitMQ
	connRabbitMQ, err := dialRabbitMQ()
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
	}
	defer connRabbitMQ.Close()

	// TCP listener
	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Error starting TCP listener: %v", err)
	}
	defer listener.Close()

	// gRPC Server
	grpcServer := grpc.NewServer()
	pbServer.RegisterServerServiceServer(grpcServer, &Server{
		connRabbitMQ: connRabbitMQ,
		listener:      listener,
	})
	log.Printf("Server is listening on port %s", os.Getenv("PORT"))
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func main() {
	initServer()
}
