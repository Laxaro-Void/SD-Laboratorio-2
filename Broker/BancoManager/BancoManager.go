package BancoManager

import (
	"log"
	"sync"
	"time"
	"context"

	"google.golang.org/grpc"

	pbBancoUSM "Broker/proto/pbBancoUSM"
)

type BancoUSM struct {
	Direction string
	Client pbBancoUSM.BancoUSMClient

	State string
	mu sync.Mutex
}

func CreateBancoManager() *BancoUSM {
	BANCO := &BancoUSM{
		State: "NONE",
	}

	go BANCO.CheckIsAliveProcedure()

	return BANCO
}

func (s *BancoUSM) CheckIsAliveProcedure() {
	for {
		time.Sleep(1 * time.Minute)
		if s.State != "NONE" {
			go func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel();
				res, err := s.Client.CheckIsAlive(ctx, &pbBancoUSM.Empty{})
				if err != nil || !res.IsAlive {
					log.Printf("[Banco] Banco at %s is not responding: %v", s.Direction, err)
					s.State = "Disconected"
					return
				}
				s.State = "Connected"
			}()
		}
	}
}

func (s *BancoUSM) Connect(IP string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State == "Connected" {
		log.Println("Aldready Connected to a alive BancoUSM")
		return false
	}

	conn := NewGRPCClient(IP)
	newClient := pbBancoUSM.NewBancoUSMClient(conn)

	_, err := newClient.CheckIsAlive(context.Background(), &pbBancoUSM.Empty{})
	if err != nil {
		log.Printf("[Banco] Fail to Conncet to new Banco: %v", err)
		return false
	}

	if s.State == "NONE" {
		s.Direction = IP
		s.Client = newClient
		s.State = "Connected"

		log.Printf("[Banco] Connected to BancoUSM at %s", IP)
		return true
	}

	if s.Direction != IP {
		conn.Close()
		log.Printf("[Banco] Reject a unknown IP")
		return false
	}

	s.State = "Connected"
	log.Printf("[Banco] Restored Conection to Banco at %s", IP)
	return true
}

func (s *BancoUSM) ProcessPayment(uuid string, amount int32, paymentMethod string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State != "Connected" {
		log.Println("Not connected to BancoUSM")
		return false, "Not connected to BancoUSM"
	}

	response, err := s.Client.ProcessPayment(context.Background(), &pbBancoUSM.PaymentRequest{
		Uuid: uuid,
		Amount: amount,
		PaymentMethod: paymentMethod,
	})
	if err != nil {
		log.Printf("Error processing payment: %v", err)
		return false, "Error processing payment"
	}

	return response.Success, response.Message
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[DB] Failed to connect to %s: %v", address, err)
	}
	return conn
}