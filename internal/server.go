package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)


type MQServer struct {
	host string 
	port int
	protocol string 
}

func CreateMQServer(host string, port int, protocol string) *MQServer {
	return &MQServer {
		host: host,
		port: port,
		protocol: protocol,
	}
}

func (server *MQServer) ListenAndServeMQ() {
	address := server.host + ":" + strconv.Itoa(server.port)
	ln, err := net.Listen(server.protocol, address)
	if err != nil {
		log.Fatalf("Error while starting MQ server on: %s", address)
		os.Exit(-1)
	}
	defer ln.Close()

	log.Printf("MQ server started on: %s", address)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln("Error while accepting connection")
			continue
		}
		go handleMessage(conn)
	}
}

type Message struct {
	msgT int16
	queue string 
	data []byte
}

func getMessage(body []byte) *Message {
	msgT := binary.BigEndian.Uint16(body[:2])
	qLen :=	binary.BigEndian.Uint32(body[2:6])

	cursor := 6 + int32(qLen)
	queue := body[6: cursor]

	dataLen := binary.BigEndian.Uint32(body[cursor:cursor+4])
	cursor += 4

	var data []byte
	if dataLen != 0 {
		data = body[cursor:cursor+int32(dataLen)]
	} else {
		data = nil
	}

	return &Message{
		msgT: int16(msgT),
		queue: string(queue),
		data: data,
	}
}

func handleMessage(conn net.Conn) {
	defer conn.Close()

	log.Printf("Connection from %s accepted", conn.RemoteAddr().String())
	var buffer bytes.Buffer

	temp := make([]byte, 1024)
	for {
		n, err := conn.Read(temp)

		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatalf("Error while reading message: %s", err)
			break
		}
		if n == 0 {
			break
		}

		buffer.Write(temp[:n])
	}
	message := getMessage(buffer.Bytes())


	log.Printf("Recieved message with type: %d, queue: %s and data: %s", message.msgT, message.queue, message.data)
}

