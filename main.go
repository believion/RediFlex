package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port 6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := newAof("database.aof")

	if err != nil {
		fmt.Println(err)
		return
	}

	defer aof.Close()

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close() // close the connection once finished

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid Command: ", command)
			return
		}

		handler(args)
	})

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.typ != "array" {
			fmt.Println("Invalid Request, Expected Array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid Request, the len of array > 0")
			continue
		}

		// the first element in the array is the command that we need to use
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := newWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid Command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}
