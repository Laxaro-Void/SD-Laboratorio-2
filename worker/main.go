package main

import (
	"fmt"
	"log"
	"os"
	"time"

	pbWorker "worker/proto/worker"

	"github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
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

type Worker struct {
	connRabbitMQ *amqp091.Connection
}

func (w *Worker) Start() {
	// Worker logic to consume tasks from RabbitMQ and process them
	// This is where you would set up the consumer and handle incoming messages
	channel, err := w.connRabbitMQ.Channel()
	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	defer channel.Close()

	// Declare the queue to consume from
	_, err = channel.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare RabbitMQ queue: %v", err)
	}
}

func (w *Worker) consumeTasks() {
	channel, err := w.connRabbitMQ.Channel()
	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	defer channel.Close()

	msgs, err := channel.Consume(
		"task_queue", // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Fatalf("Failed to register RabbitMQ consumer: %v", err)
	}

	log.Printf("Worker is waiting for tasks...")
	for msg := range msgs {
		w.processTask(msg.Body)
	}
}

func (w *Worker) processTask(taskData []byte) {
	// Logic to process the task data
	var data pbWorker.WorkerRequest
	err := proto.Unmarshal(taskData, &data)
	if err != nil {
		log.Printf("Failed to unmarshal task data: %v", err)
		return
	}

	log.Printf("Processing task with data: %d", data.Data)
}

func initWorker() {
	connRabbitMQ, err := dialRabbitMQ()
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
	}
	defer connRabbitMQ.Close()

	worker := &Worker{connRabbitMQ: connRabbitMQ}
	worker.Start()

	go worker.consumeTasks()

	// Running forever
	select {}
}

func main() {
	initWorker()
}
