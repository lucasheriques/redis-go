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

var data = make(map[string]string)

const (
	okResponse   = "+OK\r\n"
	pingResponse = "+PONG\r\n"
)

type Command struct {
	Name string
	Args []string
}

func (command *Command) executeCommand() (string, error) {
	log.Printf("Command: %s\n", strings.ToUpper(command.Name))

	switch command.Name {
	case "PING":
		return pingResponse, nil
	case "ECHO":
		return fmt.Sprintf("$%d\r\n%s\r\n", len(command.Args[0]), command.Args[0]), nil
	case "SET":
		data[command.Args[0]] = command.Args[1]
		return okResponse, nil
	case "GET":
		return fmt.Sprintf("$%d\r\n%s\r\n", len(data[command.Args[0]]), data[command.Args[0]]), nil
	}

	return "", fmt.Errorf("-ERR unknown command %#v\r\n", command)
}

func parseCommandFromNextNLines(scanner *bufio.Scanner, n int) *Command {
	var lines []string
	for i := 0; i < n; i++ {
		scanner.Scan()
		commandString := scanner.Text()

		if commandString[0] == '$' {
			_, err := strconv.Atoi(strings.Split(commandString, "$")[1])
			if err != nil {
				log.Fatal(err)
			}
		} else {
			lines = append(lines, scanner.Text())
		}
	}

	command := &Command{
		Name: strings.ToUpper(lines[0]),
		Args: lines[1:],
	}

	log.Printf("Command recv: %s\n", command)

	return command
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

			command := parseCommandFromNextNLines(scanner, numberOfLines*2)
			response, err := command.executeCommand()
			if err != nil {
				log.Fatal(err)
			}

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
