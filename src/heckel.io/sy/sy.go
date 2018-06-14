package main

import (
	"os"
	"fmt"
	"flag"
)

func check(err error, code int, message string) {
	if err != nil {
		exit(code, message)
	}
}

func exit(code int, message string) {
	fmt.Println(message)
	os.Exit(code)
}

func main() {
	serverCommand := flag.NewFlagSet("server", flag.ExitOnError)
	indexCommand := flag.NewFlagSet("index", flag.ExitOnError)

	port := serverCommand.Int("port", 8080, "Listen port for the API Server")
	api := indexCommand.String("api", "http://localhost:8080", "URL of API Server")

	if len(os.Args) < 2 {
		exit(1, "Syntax: sy (index|server)")
	}

	command := os.Args[1]

	switch command {
	case "index":
		indexCommand.Parse(os.Args[2:])
		client := NewClient(*api)
		client.Index()
	case "server":
		serverCommand.Parse(os.Args[2:])
		server := NewServer()
		server.Run(*port)
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
}