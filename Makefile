protoc:
	protoc --go_out=client/proto --go-grpc_out=client/proto client/proto/*.proto
	protoc --go_out=server/proto --go-grpc_out=server/proto server/proto/*.proto
	protoc --go_out=worker/proto --go-grpc_out=worker/proto worker/proto/*.proto

build-client:
	sudo docker-compose -f compose.yaml build client

build-server:
	sudo docker-compose -f compose.yaml build server

build-worker:
	sudo docker-compose -f compose.yaml build worker

run-rabbitmq:
	sudo docker-compose -f compose.yaml up --remove-orphans rabbitmq

run-client:
	sudo docker-compose -f compose.yaml run --use-aliases --remove-orphans client

run-server:
	sudo docker-compose -f compose.yaml run --use-aliases --remove-orphans server

run-worker:
	sudo docker-compose -f compose.yaml run --use-aliases --remove-orphans worker

clean:
	sudo docker-compose -f compose.yaml down --rmi all