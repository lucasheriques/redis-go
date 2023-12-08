package main

/**
 * This is a simple Redis server. Our goal is to implement the Redis protocol.
 */

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func getResponse() string {
	return "+PONG\r\n"
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		request := scanner.Text()

		log.Printf("Command recv: %s", request)
		if strings.ToUpper(request) == "PING" {
			conn.Write([]byte(getResponse()))
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		handleConnection(conn)
	}
}
