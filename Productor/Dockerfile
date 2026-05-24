FROM golang:1.25.10

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o server .

CMD ["./server"]
