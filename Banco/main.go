package main

import (
	"os"
	"log"
	"net"
	"time"
	"context"
	"math/rand"

	"google.golang.org/grpc"

	pbBroker "Banco/proto/pbBroker"
	pbBancoUSM "Banco/proto/pbBancoUSM"
)

type Banco struct {
	Broker pbBroker.BrokerClient
}

type BancoServer struct {
	pbBancoUSM.UnimplementedBancoUSMServer
	Banco *Banco
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
}

func (s *BancoServer) CheckIsAlive(ctx context.Context, req *pbBancoUSM.Empty) (*pbBancoUSM.IsAliveResponde, error) {
	return &pbBancoUSM.IsAliveResponde{
		IsAlive: true,
	}, nil
}

func (s *BancoServer) ProcessPayment(ctx context.Context, req *pbBancoUSM.PaymentRequest) (*pbBancoUSM.PaymentResponse, error) {
	if req.Amount <= 0 {
		return &pbBancoUSM.PaymentResponse{
			Success: false,
			Message: "Invalid payment amount",
		}, nil
	}

	if req.PaymentMethod != "debito" && req.PaymentMethod != "credito" {
		return &pbBancoUSM.PaymentResponse{
			Success: false,
			Message: "Invalid payment method",
		}, nil
	}

	var p float32;
	if req.PaymentMethod == "credito" {
		p = 0.9;
	}
	if req.PaymentMethod == "debito" {
		p = 0.8;
	}

	if rand.Float32() > p {
		log.Printf("ID: %s, Amount: %q, Method: %s, Result: Fail\n", req.Uuid, req.Amount, req.PaymentMethod)
		return &pbBancoUSM.PaymentResponse{
			Success: false,
			Message: "Payment failed due to insufficient funds or other issues",
		}, nil
	}

	log.Printf("ID: %s, Amount: %q, Method: %s, Result: Success\n", req.Uuid, req.Amount, req.PaymentMethod)
	return &pbBancoUSM.PaymentResponse{
		Success: true,
		Message: "Payment successful",
	}, nil
}

func (s *Banco) Handshake() bool {
	conn := NewGRPCClient(os.Getenv("BROKER_URL"))
	s.Broker = pbBroker.NewBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	res, err := s.Broker.Handshake(ctx, &pbBroker.HandshakeRequest{
		Direction: os.Getenv("MYDIRECTION"),
		WhatIAm: "BANCO",
	})
	if err != nil || !res.Success {
		log.Printf("Faild to do Handshake: %v", err)
		return false
	}

	_, err = s.Broker.CheckIsAlive(context.Background(), &pbBroker.Empty{})
	if err != nil {
		log.Printf("Faild to check Alive: %v", err)
		return false
	}

	log.Print("Conection Succsess")
	return true
}

func StartBancoServer(banco *Banco) {
	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pbBancoUSM.RegisterBancoUSMServer(grpcServer, &BancoServer{
		Banco: banco,
	})

	log.Printf("Servers is listening on %s", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func serverBackground() {
	banco := &Banco{

	}

	go StartBancoServer(banco)
	time.Sleep(1 * time.Second)
	
	for !banco.Handshake() {
		time.Sleep(2 * time.Second)
	}

	forever := make(chan bool)
	log.Printf("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func main() {
	time.Sleep(1 * time.Second)
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	hostname, _ := os.Hostname()
	log.Printf("%s is running on port %s, Hostname: %s", name, port, hostname)

	serverBackground()
}
