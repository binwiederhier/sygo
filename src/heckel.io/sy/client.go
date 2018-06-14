package main

import (
	"os"
	"fmt"
	"bufio"
	"io"
	"crypto/md5"
	"encoding/hex"
	"math/rand"
	"path/filepath"
	"strings"
	"time"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"bytes"
)

func indexFile(index *index, filename string) []string {
	file, err := os.Open(filename)
	check(err, 1, "Cannot open file")

	defer file.Close()

	index.RemoveFile(filename)

	fileId := rand.Int()

	buffer := make([]byte, 512*1024)
	reader := bufio.NewReader(file)

	num := 0
	chunks := make([]string, 0)

	for {
		read, err := reader.Read(buffer)

		if err == io.EOF {
			break
		} else if err != nil {
			exit(2, "Cannot read chunkFile " + filename)
		}

		checksumBytes := md5.Sum(buffer[:read])
		checksum := hex.EncodeToString(checksumBytes[:])

		index.AddFileChunk(fileId, checksum, num)
		num++

		if !index.Exists(checksum) {
			chunks = append(chunks, checksum)

			index.AddChunk(checksum)
			index.WriteChunk(checksum, buffer[:read])
		}
	}

	index.AddFile(fileId, "", filename)

	return chunks
}

func runIndex() {
	root := ".sy"
	index, err := NewIndex(root)
	check(err, 1, "Cannot open index")

	defer index.Close()

	err = index.Load()
	check(err, 1, "Cannot load index")

	err = index.Begin()
	check(err, 1, "Cannot start index transaction")

	rand.Seed(time.Now().Unix())

	chunks := make([]string, 0)

	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && !strings.HasPrefix(path, root) {
			newChunks := indexFile(index, path)

			if len(newChunks) > 0 {
				fmt.Printf("Indexed %s using %d new chunk(s)\n", path, len(newChunks))
				chunks = append(chunks, newChunks...)
			}

			if len(chunks) > 10 {
				fmt.Println("10 chunks, uploading...")
				upload(index, chunks)
			}
		}

		return nil
	})

	index.Commit()
}

func upload(index *index, chunks []string) {
	request := NewJsonRpcRequest("v1/chunks/diff", map[string]interface{}{
		"chunks": chunks,
	})

	requestStr, err := json.Marshal(request)
	check(err, 1, "Cannot create JSON array")

	responseStr, err := http.Post("http://localhost:8080/api", "application/json", bytes.NewBuffer(requestStr))
	check(err, 2, "Cannot check chunks API")

	body, err := ioutil.ReadAll(responseStr.Body)
	check(err, 3, "Cannot read body")

	response, err := ParseJsonRpcResponse(body)
	check(err, 4, "Cannot parse response")

	fmt.Println(response)

	if unknownChunks, ok := response.Result["unknown"]; ok {
		for _, checksum := range unknownChunks.([]interface{}) {
			checksumStr := checksum.(string)

			chunkBytes, err := index.ReadChunk(checksumStr)
			check(err, 5, "Cannot read chunk " + checksumStr)

			fmt.Println("Uploading", checksumStr)
			_, err = http.Post("http://localhost:8080/api/upload/" + checksumStr, "application/octet-stream", bytes.NewReader(chunkBytes))
			check(err, 6, "Bad response")

			index.DeleteChunk(checksumStr)
		}
	}

}
