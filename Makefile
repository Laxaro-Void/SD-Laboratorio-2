# Makefile #
build-protoc:
	protoc --go_out=Banco/proto --go-grpc_out=Banco/proto Banco/proto/*.proto
	protoc --go_out=Broker/proto --go-grpc_out=Broker/proto Broker/proto/*.proto
	protoc --go_out=Consumidor/proto --go-grpc_out=Consumidor/proto Consumidor/proto/*.proto
	protoc --go_out=Productor/proto --go-grpc_out=Productor/proto Productor/proto/*.proto
	protoc --go_out=DynamoDB/proto --go-grpc_out=DynamoDB/proto DynamoDB/proto/*.proto

## Production
VM1:

VM2:

VM3:

VM4:


## Localhost
localhost-VM1:
	sudo docker-compose -f compose.localhost.yaml build broker
	sudo docker-compose -f compose.localhost.yaml up --remove-orphans broker

localhost-VM2:
	sudo docker compose -f compose.localhost.yaml build productor1 productor2 productor3 productor4 dynamodb
	sudo docker compose -f compose.localhost.yaml up --remove-orphans -d productor1
	sudo docker compose -f compose.localhost.yaml up --remove-orphans -d productor2
	sudo docker compose -f compose.localhost.yaml up --remove-orphans -d productor3
	sudo docker compose -f compose.localhost.yaml up --remove-orphans -d productor4

	sudo docker compose -f compose.localhost.yaml up --remove-orphans -d dynamodb
	sudo docker compose -f compose.localhost.yaml logs -f productor1 productor2 productor3 productor4 dynamodb

localhost-VM3:
	sudo docker-compose -f compose.localhost.yaml build consumidor1 consumidor2 dynamodb

	sudo docker compose -f compose.localhost.yaml up --remove-orphans -d consumidor1
	sudo docker compose -f compose.localhost.yaml up --remove-orphans -d consumidor2

	sudo docker-compose -f compose.localhost.yaml up --remove-orphans -d dynamodb
	sudo docker-compose -f compose.localhost.yaml logs -f consumidor1 consumidor2 dynamodb

localhost-VM4:
	sudo docker-compose -f compose.localhost.yaml build banco dynamodb
	sudo docker-compose -f compose.localhost.yaml up --remove-orphans -d banco dynamodb
	sudo docker-compose -f compose.localhost.yaml logs -f banco dynamodb


