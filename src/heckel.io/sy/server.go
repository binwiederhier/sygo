package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"strings"
)

type Server struct {
	index *index
}

func NewServer() Server {
	index, err := NewIndex("/tmp/sy")
	check(err, 1, "Cannot open database")

	err = index.Load()
	check(err, 2, "Cannot load index")

	return Server{
		index: index,
	}
}

func (server *Server) exit(w http.ResponseWriter, code int, message string) {
	response, _ := json.Marshal(map[string]string{
		"error": message,
	})

	w.WriteHeader(code)
	w.Write([]byte(response))
}

func (server *Server) serveUpload(w http.ResponseWriter, r *http.Request) {
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

func (server *Server) serveUploadMulti(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)

	r.ParseForm()

	for key, value := range r.Form {
		fmt.Printf("%s = %s\n", key, value)
	}
	/*reader, err := r.MultipartReader()
	form, err := reader.ReadForm(5 * 1024 * 1024)

	bodyBytes, _ := ioutil.ReadAll(r.Body)
	fmt.Println(string(bodyBytes))*/
}

func (server *Server) serveDiff(w http.ResponseWriter, r *http.Request) {
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

func (server *Server) Run(port int) {
	http.HandleFunc("/api/diff", server.serveDiff)
	http.HandleFunc("/api/upload", server.serveUploadMulti)
	http.HandleFunc("/api/upload/", server.serveUpload)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
