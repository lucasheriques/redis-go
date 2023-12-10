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
	"strconv"
	"strings"
)

func getResponse() string {
	return "+PONG\r\n"
}

func executeCommand(command []string) string {
	switch strings.ToUpper(command[0]) {
	case "PING":
		return "+PONG\r\n"
	case "ECHO":
		return fmt.Sprintf("$%d\r\n%s\r\n", len(command[1]), command[1])
	}

	return "-ERR unknown command '" + command[0] + "'\r\n"
}

func parseNextNLines(scanner *bufio.Scanner, n int) []string {
	var lines []string
	for i := 0; i < n; i++ {
		scanner.Scan()
		command := scanner.Text()

		if command[0] == '$' {
			_, err := strconv.Atoi(strings.Split(command, "$")[1])
			if err != nil {
				log.Fatal(err)
			}
		} else {
			lines = append(lines, scanner.Text())
		}
	}
	return lines
}

func handleConnection(conn net.Conn) {
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		request := scanner.Text()

		if request[0] == '*' {
			numberOfLines, err := strconv.Atoi(strings.Split(request, "*")[1])
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Number of lines: %d\n", numberOfLines)

			command := parseNextNLines(scanner, numberOfLines*2)
			log.Printf("Command recv: %s", command)

			response := executeCommand(command)

			conn.Write([]byte(response))
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
		defer conn.Close()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}
