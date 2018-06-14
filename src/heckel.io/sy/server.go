package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"strings"
)

type server struct {
	index *index
}

func NewServer() server {
	index, err := NewIndex("/tmp/sy")
	check(err, 1, "Cannot open database")

	err = index.Load()
	check(err, 2, "Cannot load index")

	return server{
		index: index,
	}
}

func (server *server) exit(w http.ResponseWriter, code int, message string) {
	response, _ := json.Marshal(JsonRpcResponse{Error: message})

	w.WriteHeader(code)
	w.Write([]byte(response))
}

func (server *server) serveApi(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api" {
		body, err := ioutil.ReadAll(r.Body)

		if err != nil {
			server.exit(w, 400, "Bad Request")
			return
		}

		request, err := ParseJsonRpcRequest(body)

		if err != nil {
			server.exit(w, 400, "Invalid JSON")
			return
		}

		var response JsonRpcResponse

		switch request.Method {
		case "v1/chunks/diff":
			response, err = server.chunksDiff(request)
		default:
			server.exit(w, 405, "Unknown method")
		}

		if err != nil {
			server.exit(w, 501, "Server failure")
			return
		}

		bytes, err := json.Marshal(response)

		if err != nil {
			server.exit(w, 500, "Server failure")
			return
		}

		w.Write(bytes)
		fmt.Println("chunks: ", request)
	}
}

func (server *server) serveUpload(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	checksum := strings.TrimPrefix(r.URL.Path, "/api/upload/")
fmt.Println(checksum)
	if len(checksum) != 32 {
		server.exit(w, 400, "Bad Request")
		return
	}

	if !server.index.Exists(checksum) {
		chunkBytes, err := ioutil.ReadAll(r.Body)

		if err != nil {
			server.exit(w, 400, "Cannot read request")
		}

		server.index.Begin()
		server.index.AddChunk(checksum)
		server.index.WriteChunk(checksum, chunkBytes)
		server.index.Commit()
	}
}

func (server *server) chunksDiff(request JsonRpcRequest) (JsonRpcResponse, error) {
	unknown := make([]string, 0)
	chunks := request.Params["chunks"].([]interface{})

	for _, checksum := range chunks {
		if !server.index.Exists(checksum.(string)) {
			unknown = append(unknown, checksum.(string))
		}
	}

	return JsonRpcResponse{Result: map[string]interface{}{
		"unknown": unknown,
	}}, nil
}

func (server *server) run() {
	http.HandleFunc("/api/upload/", server.serveUpload)
	http.HandleFunc("/api", server.serveApi)
	http.ListenAndServe(":8080", nil)
}

func runServer() {
	server := NewServer()
	server.run()
}