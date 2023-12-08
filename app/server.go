package main

/**
 * This is a simple Redis server. Our goal is to implement the Redis protocol.
 */

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func getResponse(request string) string {
	return "+PONG\r\n"
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		request, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("Received: %s\n", request)

		response := getResponse(request)
		_, err = writer.WriteString(response)
		if err != nil {
			fmt.Println("Error writing: ", err.Error())
			os.Exit(1)
		}
		writer.Flush()
	}
}
