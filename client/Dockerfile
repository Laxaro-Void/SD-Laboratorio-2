FROM golang:1.24.2

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o client .

RUN mkdir -p /app/output

CMD ["./client"]
