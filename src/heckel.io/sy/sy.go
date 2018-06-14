package main

import (
	"os"
	"fmt"
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
	if len(os.Args) < 2 {
		exit(1, "Syntax: sy (index|server)")
	}

	command := os.Args[1]

	switch command {
	case "index":
		runIndex()
	case "server":
		runServer()
	}
}