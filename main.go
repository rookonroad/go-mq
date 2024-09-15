package main

import (
	"github.com/rookonroad/go-mq/internal"
)

func main() {
	var server = internal.CreateMQServer("localhost", 9041, "tcp")
	server.ListenAndServeMQ()
}