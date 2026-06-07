package main

import (
	"bufio"
	"context"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	pbBancoUSM "Banco/proto/pbBancoUSM"
	pbRegisterAuth "Banco/proto/pbRegisterAuth"
)

type Banco struct {
	BrokerConnection *grpc.ClientConn
	RegisterAuthClient pbRegisterAuth.RegisterAuthClient

	KeyPath string
	uuid string
}

type BancoServer struct {
	pbBancoUSM.UnimplementedBancoUSMServer
	node *Banco
}

func (c *Banco) SaveUUID(uuid string) {
	file, err := os.Create(c.KeyPath)
	if err != nil {
		log.Fatalf("Error Creating file: %v", err)
	}
	defer file.Close()

	file.WriteString(uuid + "\n")
	log.Println("Key Saved, Now you can safely close your sesion")
}

func (c *Banco) ReadUUID() (string, bool) {
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

func (c *Banco) Register(username string) (string, bool) {
	message := pbRegisterAuth.RegisterRequest{
		Username: username,
		Type:     "DB",
	}

	response, err := c.RegisterAuthClient.Register(context.Background(), &message)
	if err != nil {
		log.Printf("Error registering Node: %v", err)
		return "", false
	}

	if !response.Success {
		log.Printf("Failed to register Node %s: %s", username, response.Message)
		return "", false
	}

	log.Printf("Node %s registered successfully: UUID=%s", username, response.Uuid)
	return response.Uuid, true
}

func (c *Banco) Authenciate(username string, uuid string) bool {
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

func (c *Banco) RequestSession() bool {
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

func (c *Banco) RegisterNode() bool {
	uuid, _ := c.ReadUUID()
	message := &pbRegisterAuth.RegisterNodeRequest{
		Uuid: uuid,
		Direction: os.Getenv("MYDIRECTION"),
	}

	response, err := c.RegisterAuthClient.RegisterBanco(context.Background(), message)
	if err != nil {
		log.Fatalf("Error to Register Banco, %v", err)
		return false
	}

	if !response.Succes {
		log.Fatalf("Broker regect the Banco: %s", response.Message)
		return false
	}

	log.Println("Borker Conected to Banco")
	return true
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	return conn
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

func StartBancoServer(listener net.Listener, node *Banco) {
	NodeServer := &BancoServer{
		node: node,
	}

	grpcServer := grpc.NewServer()
	pbBancoUSM.RegisterBancoUSMServer(grpcServer, NodeServer)

	log.Printf("NodeServer is listening on %s", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func initBanco() {
	BrokerConnection := NewGRPCClient(os.Getenv("BROKER_URL"))
	defer BrokerConnection.Close()

	banco := &Banco{
		BrokerConnection: BrokerConnection,
		RegisterAuthClient: pbRegisterAuth.NewRegisterAuthClient(BrokerConnection),
		KeyPath: "keys/" + os.Getenv("NAME") + "-key.txt",
	}

	listener, err := net.Listen("tcp", ":"+os.Getenv("PORT"))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go StartBancoServer(listener, banco)
	time.Sleep(2 * time.Second)
	
	if !banco.RequestSession() {
		panic("Unable to request Session")
	}

	banco.RegisterNode()
	log.Println("Conexion estable to broker")

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

	initBanco()
}
