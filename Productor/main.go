package main

import (
	"time"
	"log"
	"os"
)

func main() {
	time.Sleep(5 * time.Second)
	log.Println("Productor started")
	log.Println("Productor Hostname: " + os.Getenv("NAME"))
}
