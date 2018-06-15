package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"mime/multipart"
)

type Server struct {
	idx *index
}

func NewServer() Server {
	index, err := NewIndex("/tmp/sy")
	check(err, 1, "Cannot open database")

	err = index.Load()
	check(err, 2, "Cannot load index")

	return Server{
		idx: index,
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

	err := r.ParseMultipartForm(50 * 1024 * 1024)

	if err != nil {
		server.exit(w, 400, "Cannot read multipart request")
	}

	fmt.Println(r)
	server.idx.Begin()

	for _, headers := range r.MultipartForm.File {
		for _, hdr := range headers {
			checksum := hdr.Filename

			if len(checksum) != 32 {
				server.exit(w, 400, "Bad request")
				return
			}

			var reader multipart.File
			if reader, err = hdr.Open(); nil != err {
				server.exit(w, 500, "Invalid request (1)")
				return
			}

			server.idx.WriteChunkFromReader(checksum, reader)
			server.idx.AddChunk(checksum)

			fmt.Println("-> uploaded", checksum)
		}
	}

	server.idx.Commit()
}

func (server *Server) serveDiff(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	body, err := ioutil.ReadAll(r.Body)
	fmt.Println("<-", string(body))

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
		if !server.idx.Exists(checksum) {
			unknown = append(unknown, checksum)
		}
	}

	response, err := json.Marshal(unknown)

	if err != nil {
		server.exit(w, 500, "Cannot process unknown chunks")
		return
	}

	fmt.Println("->", string(response))
	w.Write([]byte(response))
}

func (server *Server) Run(port int) {
	http.HandleFunc("/api/diff", server.serveDiff)
	http.HandleFunc("/api/upload", server.serveUpload)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
