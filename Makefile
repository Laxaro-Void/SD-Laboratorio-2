# Makefile #
build-protoc:
	protoc --go_out=Banco/proto --go-grpc_out=Banco/proto Banco/proto/*.proto
	protoc --go_out=Broker/proto --go-grpc_out=Broker/proto Broker/proto/*.proto
	protoc --go_out=DynamoDB/proto --go-grpc_out=DynamoDB/proto DynamoDB/proto/*.proto
	protoc --go_out=Productor/proto --go-grpc_out=Productor/proto Productor/proto/*.proto
	protoc --go_out=Consumer/proto --go-grpc_out=Consumer/proto Consumer/proto/*.proto

run-broker:
	sudo docker compose build broker
	sudo docker compose up --remove-orphans broker

stop-broker:
	sudo docker compose stop broker

run-banco:
	sudo docker compose build banco
	sudo docker compose up --remove-orphans banco

run-node-1:
	sudo docker compose build node1
	sudo docker compose up --remove-orphans node1

run-node-2:
	sudo docker compose build node2
	sudo docker compose up --remove-orphans node2

run-node-3:
	sudo docker compose build node3
	sudo docker compose up --remove-orphans node3

stop-node-1:
	sudo docker compose stop node1

stop-node-2:
	sudo docker compose stop node2

stop-node-3:
	sudo docker compose stop node3

run-producer:
	sudo docker compose build productor1 productor2 productor3 productor4
	sudo docker compose up --remove-orphans productor1 productor2 productor3 productor4

stop-producer:
	sudo docker compose stop productor1 productor2 productor3 productor4