# Makefile #
build-protoc:
	protoc --go_out=Banco/proto --go-grpc_out=Banco/proto Banco/proto/*.proto
	protoc --go_out=Broker/proto --go-grpc_out=Broker/proto Broker/proto/*.proto
	protoc --go_out=DynamoDB/proto --go-grpc_out=DynamoDB/proto DynamoDB/proto/*.proto
	protoc --go_out=Productor/proto --go-grpc_out=Productor/proto Productor/proto/*.proto
	protoc --go_out=Consumer/proto --go-grpc_out=Consumer/proto Consumer/proto/*.proto

# Production
docker-VM1:
	sudo docker-compose -f compose.yaml build broker
	sudo docker-compose -f compose.yaml up broker
docker-VM2:
	sudo docker-compose -f compose.yaml build productor1 productor2 productor3 productor4 node3
	sudo docker-compose -f compose.yaml up productor1 productor2 productor3 productor4 node3

docker-VM3:
	sudo docker-compose -f compose.yaml build consumer1 consumer1 node2
	sudo docker-compose -f compose.yaml up consumer1 consumer1 node2

docker-VM4:
	sudo docker-compose -f compose.yaml build banco node1
	sudo docker-compose -f compose.yaml up banco node1

# LocalHost Only
run-broker:
	sudo docker compose -f local-compose.yaml build broker
	sudo docker compose -f local-compose.yaml up --remove-orphans broker

stop-broker:
	sudo docker compose -f local-compose.yaml stop broker

run-banco:
	sudo docker compose -f local-compose.yaml build banco
	sudo docker compose -f local-compose.yaml up --remove-orphans banco

run-node-1:
	sudo docker compose -f local-compose.yaml build node1
	sudo docker compose -f local-compose.yaml up --remove-orphans node1

run-node-2:
	sudo docker compose -f local-compose.yaml build node2
	sudo docker compose -f local-compose.yaml up --remove-orphans node2

run-node-3:
	sudo docker compose -f local-compose.yaml build node3
	sudo docker compose -f local-compose.yaml up --remove-orphans node3

stop-node-1:
	sudo docker compose -f local-compose.yaml stop node1

stop-node-2:
	sudo docker compose -f local-compose.yaml stop node2

stop-node-3:
	sudo docker compose -f local-compose.yaml stop node3

run-producer:
	sudo docker compose -f local-compose.yaml build productor1 productor2 productor3 productor4
	sudo docker compose -f local-compose.yaml up --remove-orphans productor1 productor2 productor3 productor4

stop-producer:
	sudo docker compose -f local-compose.yaml stop productor1 productor2 productor3 productor4

run-consumer:
	sudo docker compose -f local-compose.yaml build consumer1 consumer2
	sudo docker compose -f local-compose.yaml up --remove-orphans consumer1 consumer2

stop-consumer:
	sudo docker compose -f local-compose.yaml stop consumer1 consumer2