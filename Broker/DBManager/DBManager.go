package DBManager

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pbDynamoDB "Broker/proto/pbDynamoDB"
)

type Node struct {
	Direction string
	Client pbDynamoDB.DynamoDBClient
	State string
	IsSync bool
}

type DBManager struct {
	mu sync.Mutex

	// Configuration
	N int
	W int
	R int

	// General State
	FirstWrite bool

	Nodes []Node
	NodesMap map[string]int

	// Stats
	W_Exitosas int
	W_Fallidas int

}

// Crea un Gestor de BD
func CreateDBManager(N int, W int, R int) *DBManager {
	if 2*W <= N || 2*R <= N {
		log.Fatalf("[DB] Invalid Configuration")
		return nil
	}

	DB := &DBManager{
		N: N,
		W: W,
		R: R,

		FirstWrite: false,

		NodesMap: make(map[string]int),
	}

	go DB.ChekIsAliveProcedure()

	return DB
}

// Verifica si los nodos existentes estan vivos
func (s *DBManager) ChekIsAliveProcedure() {
	for {
		time.Sleep(1 * time.Minute)

		for idx, node := range s.Nodes {
			go func(node Node) {
				s.mu.Lock()
				defer s.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel();
				res, err := node.Client.CheckIsAlive(ctx, &pbDynamoDB.Empty{})
				if err != nil || !res.IsAlive {
					log.Printf("[DB] Node %s is not responding: %v", node.Direction, err)
					s.Nodes[idx].State = "Disconected"
					return
				}
				s.Nodes[idx].State = "Connected"
			}(node)
		}
	}
}

// Agrega un nuevo nodo entrante
func (s *DBManager) AddConection(IP string) bool {
	conn := NewGRPCClient(IP)
	newNode := &Node{
		Direction: IP,
		Client: pbDynamoDB.NewDynamoDBClient(conn),
		State: "New",
		IsSync: false,
	}

	_, err := newNode.Client.CheckIsAlive(context.Background(), &pbDynamoDB.Empty{})
	if err != nil {
		log.Printf("[DB] Fail to Conncet to newNode: %v", err)
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	newNode.State = "Connected"
	idx, exists := s.NodesMap[IP]

	// Restauracion de conexion
	if (exists) {
		s.Nodes[idx].IsSync = false
		s.Nodes[idx].State = "Connected"
		log.Printf("[DB] Restored Connection to Node %s", IP)

		go s.SyncNodes()
		return true
	}

	// Acepta el nodo si hay menos de N
	if len(s.Nodes) < s.N {
		if !s.FirstWrite {
			newNode.IsSync = true
		}
		s.NodesMap[IP] = len(s.Nodes)
		s.Nodes = append(s.Nodes, *newNode)
		log.Printf("[DB] Added new conexion IP=%s", IP)

		go s.SyncNodes()
		return true
	}

	// Rechaza nuevos entrantes al superar N
	conn.Close()
	log.Printf("[DB] Limited to %d Nodes, Rejecting %s", s.N, IP)
	return false
}

// Sinconiza los nodos que necesitan
func (s *DBManager) SyncNodes() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var needSync = false
	for _, node := range s.Nodes {
		if !node.IsSync {
			needSync = true
			break
		}
	}

	if !needSync {
		return
	}

	// Encuentra el primer nodo que esta sync
	var sourceIndex = -1
	for idx, node := range s.Nodes {
		if node.IsSync && node.State == "Connected" {
			sourceIndex = idx
			break
		}
	}

	if sourceIndex == -1 {
		log.Panic("[DB] No sync source node available")
		return
	}

	// Obiene toda la data de la conexion
	sourceNode := s.Nodes[sourceIndex]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	allData, err := sourceNode.Client.GetAllData(ctx, &pbDynamoDB.Empty{})
	if err != nil {
		log.Printf("[DB] Failed to retrieve data from node %s: %v", sourceNode.Direction, err)
		return
	}

	// Recorre los ndos conocidos en busca de sincronizacion
	for idx, node := range s.Nodes {
		if idx == sourceIndex || node.IsSync || node.State != "Connected" {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := node.Client.SyncAllData(ctx, &pbDynamoDB.SyncAllDataRequest{
			AllData: allData.AllData,
		})
		cancel()

		if err != nil {
			log.Printf("[DB] Failed to sync node %s: %v", node.Direction, err)
			continue
		}

		if res == nil || !res.Success {
			log.Printf("[DB] Failed to sync node %s: sync not acknowledged", node.Direction)
			continue
		}

		s.Nodes[idx].IsSync = true
		log.Printf("[DB] Node %s synced successfully", node.Direction)
	}
}

// Se solicita a los nodos crear una tabla
func (s *DBManager) CreateTable(tableName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Nodes) < s.W  {
		log.Printf("[DB] Failed to create table %s: No nodes available\n", tableName)
		return false
	}

	var successNodes []int
	var failedNodes []int

	// Recorre los nodos existentes
	for idx, node := range s.Nodes {
		if node.State == "Connected" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel();
			res, err := node.Client.CreateTable(ctx, &pbDynamoDB.CreateTableRequest{
				TableName: tableName,
			})
			if err != nil {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to create table %s on node %s: %v\n", tableName, node.Direction, err)

			} else if !res.Success {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to create table %s on node %s: %v\n", tableName, node.Direction, err)

			} else {
				successNodes = append(successNodes, idx)
			}
		} else {
			log.Printf("[DB] Failed to create table %s on node %s: Node is not connected\n", tableName, node.Direction)
			failedNodes = append(failedNodes, idx)
		}
	}

	// Requerimiento de escritura
	if len(successNodes) < s.W {
		for _, idx := range successNodes {
			s.Nodes[idx].IsSync = false
		}

		log.Printf("[DB] Failed to create table %s: Some nodes failed to create the table\n", tableName)
		return false
	}

	log.Printf("[DB] Successfully created table %s on nodes: %v\n", tableName, successNodes)
	for _, idx := range failedNodes {
		s.Nodes[idx].IsSync = false
	}

	// Solicita Sync Asincrono
	go s.SyncNodes()

	if !s.FirstWrite { s.FirstWrite = true }
	return true
}

// Se gestiona poner un item
// Notar que se generaliza el valor como una cadena de Bytes.
func (s *DBManager) PutItem(tableName string, key string, value []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Nodes) < s.W  {
		log.Printf("[DB] Failed to put item %s: No nodes available", tableName)
		return false
	}

	var successNodes []int
	var failedNodes []int

	// Mismo Patron de diseno de escritura
	for idx, node := range s.Nodes {
		if node.State == "Connected" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel();
			res, err := node.Client.PutItem(ctx, &pbDynamoDB.PutItemRequest{
				TableName: tableName,
				Key: key,
				Value: value,
			})
			if err != nil {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to put item in table %s on node %s: %v", tableName, node.Direction, err)

			} else if !res.Success {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to put item in table %s on node %s", tableName, node.Direction)

			} else {
				successNodes = append(successNodes, idx)
			}
		} else {
			log.Printf("[DB] Failed to put item in table %s on node %s: Node is not connected", tableName, node.Direction)
			failedNodes = append(failedNodes, idx)
		}
	}

	if len(successNodes) < s.W {
		for _, idx := range successNodes {
			s.Nodes[idx].IsSync = false
		}

		log.Printf("[DB] Failed to put item in table %s: Some nodes failed to create the table", tableName)
		return false
	}

	log.Printf("[DB] Successfully put item in table %s on nodes: %v\n", tableName, successNodes)
	for _, idx := range failedNodes {
		s.Nodes[idx].IsSync = false
	}

	//Peticion de sync eventual
	go s.SyncNodes()

	if !s.FirstWrite { s.FirstWrite = true }
	return true
}

// Peticion de obtener toda la tabla
func (s *DBManager) GetTable(tableName string) (*pbDynamoDB.Table, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if (len(s.Nodes) < s.R) {
		log.Printf("[DB] Failed to get item from table %s: No nodes available", tableName)
		return &pbDynamoDB.Table{
			Name: "",
			Keys: nil,
			Value: nil,
		}, false
	}

	var successNodes []int
	var successResults [][]byte
	var failedNodes []int

	// Se obtienen los resultados de los nodos positivos
	for idx, node := range s.Nodes {
		if node.State == "Connected" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel();
			res, err := node.Client.GetTable(ctx, &pbDynamoDB.GetTableRequest{
				TableName: tableName,
			})
			if err != nil {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to get table %s on node %s: %v", tableName, node.Direction, err)

			} else if !res.Success {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to get table %s on node %s", tableName, node.Direction)

			} else {
				value, err := proto.Marshal(res.Data)
				if err != nil {
					log.Panicln("What")
				}
				successNodes = append(successNodes, idx)
				successResults = append(successResults, value)
			}
		} else {
			log.Printf("[DB] Failed to get table %s on node %s: Node is not connected", tableName, node.Direction)
			failedNodes = append(failedNodes, idx)
		}
	}
	if len(successResults) < s.W {
		log.Printf("[DB] Failed to get item in table %s: Some nodes failed to get item", tableName)
		return nil, false
	}

	counts := make(map[string]int)
	values := make(map[string][]byte)

	// Counts Times Same
	for _, value := range successResults {
		valueKey := string(value)
		counts[valueKey]++
		if _, ok := values[valueKey]; !ok {
			values[valueKey] = value
		}
	}

	// Get Most repeted
	mostCount := 0
	var mostValue []byte
	for key, count := range counts {
		if count > mostCount {
			mostCount = count
			mostValue = values[key]
		}
	}

	// Cual es el valor que mas se repite es el valor real.
	if mostCount < s.R {
		log.Printf("[DB] The Table %s is not repeated", tableName)
		go s.SyncNodes()

		return nil, false
	}

	log.Printf("[DB] Successfully got table %s on nodes: %v", tableName, successNodes)
	for idx, value := range successResults {
		valueKey := string(value)
		if  c, _ := counts[valueKey]; c < mostCount {
			s.Nodes[successNodes[idx]].IsSync = false
		}
	}

	go s.SyncNodes()

	res := &pbDynamoDB.Table{}
	err := proto.Unmarshal(mostValue, res)
	if err != nil {
		log.Panicln("What")
	}

	return res, true
}

// Se obtiene un item a partir de una llave y tabla
func (s *DBManager) GetItem(tableName string, key string) (*pbDynamoDB.Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if (len(s.Nodes) < s.R) {
		log.Printf("[DB] Failed to get item from table %s: No nodes available", tableName)
		return &pbDynamoDB.Item{
			Value: nil,
			Exists: false,
		}, false
	}

	var successNodes []int
	var successResults [][]byte
	var failedNodes []int

	// Mismo patron de lectura
	for idx, node := range s.Nodes {
		if node.State == "Connected" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel();
			res, err := node.Client.GetItem(ctx, &pbDynamoDB.GetItemRequest{
				TableName: tableName,
				Key: key,
			})
			if err != nil {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to get item in table %s on node %s: %v", tableName, node.Direction, err)

			} else if !res.Success {
				failedNodes = append(failedNodes, idx)
				log.Printf("[DB] Failed to get item in table %s on node %s", tableName, node.Direction)

			} else {
				value, err := proto.Marshal(res.Value)
				if err != nil {
					log.Panicln("What")
				}
				successNodes = append(successNodes, idx)
				successResults = append(successResults, value)
			}
		} else {
			log.Printf("[DB] Failed to get item in table %s on node %s: Node is not connected", tableName, node.Direction)
			failedNodes = append(failedNodes, idx)
		}
	}

	if len(successResults) < s.W {
		log.Printf("[DB] Failed to get item in table %s: Some nodes failed to get item", tableName)
		return nil, false
	}

	counts := make(map[string]int)
	values := make(map[string][]byte)

	// Counts Times Same
	for _, value := range successResults {
		valueKey := string(value)
		counts[valueKey]++
		if _, ok := values[valueKey]; !ok {
			values[valueKey] = value
		}
	}

	// Get Most repeted
	mostCount := 0
	var mostValue []byte
	for key, count := range counts {
		if count > mostCount {
			mostCount = count
			mostValue = values[key]
		}
	}

	if mostCount < s.R {
		log.Printf("[DB] The Got item is not repeted in table %s at key %s", tableName, key)
		go s.SyncNodes()

		return nil, false
	}

	log.Printf("[DB] Successfully got item in table %s on nodes: %v", tableName, successNodes)
	for idx, value := range successResults {
		valueKey := string(value)
		if  c, _ := counts[valueKey]; c < mostCount {
			s.Nodes[successNodes[idx]].IsSync = false
		}
	}

	go s.SyncNodes()

	res := &pbDynamoDB.Item{}
	err := proto.Unmarshal(mostValue, res)
	if err != nil {
		log.Panicln("What")
	}

	return res, true
}

func NewGRPCClient(address string) *grpc.ClientConn {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[DB] Failed to connect to %s: %v", address, err)
	}
	return conn
}