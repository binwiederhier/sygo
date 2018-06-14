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

func newServer() server {
	index, err := NewIndex("/tmp/sy")
	check(err, 1, "Cannot open database")

	err = index.Load()
	check(err, 2, "Cannot load index")

	return server{
		index: index,
	}
}

func (server *server) exit(w http.ResponseWriter, code int, message string) {
	response, _ := json.Marshal(map[string]string{
		"error": message,
	})

	w.WriteHeader(code)
	w.Write([]byte(response))
}

func (server *server) serveUpload(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	checksum := strings.TrimPrefix(r.URL.Path, "/api/upload/")

	if len(checksum) != 32 {
		server.exit(w, 400, "Bad request")
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

func (server *server) serveDiff(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		server.exit(w, 400, "Cannot read request")
		return
	}

	var chunks []string
	err = json.Unmarshal(body, &chunks)

	if err != nil {
		server.exit(w, 400, "Invalid JSON")
		return
	}

	unknown := make([]string, 0)

	for _, checksum := range chunks {
		if !server.index.Exists(checksum) {
			unknown = append(unknown, checksum)
		}
	}

	response, err := json.Marshal(unknown)

	if err != nil {
		server.exit(w, 500, "Cannot process unknown chunks")
		return
	}

	w.Write([]byte(response))
}

func (server *server) run() {
	http.HandleFunc("/api/upload/", server.serveUpload)
	http.HandleFunc("/api/diff", server.serveDiff)
	http.ListenAndServe(":8080", nil)
}

func runServer() {
	server := newServer()
	server.run()
}