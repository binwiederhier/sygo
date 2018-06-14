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
			chunks = append(chunks, newChunks...)

			if len(chunks) > 50 {
				upload(index, chunks)
				chunks = make([]string, 0)
			}
		}

		return nil
	})

	if len(chunks) > 0 {
		upload(index, chunks)
	}

	index.Commit()
}

func upload(index *index, chunks []string) {
	chunkListJson, err := json.Marshal(chunks)
	check(err, 1, "Cannot convert to JSON")

	responseStr, err := http.Post("http://localhost:8080/api/diff", "application/json", bytes.NewBuffer(chunkListJson))
	check(err, 2, "Cannot check chunks API")

	body, err := ioutil.ReadAll(responseStr.Body)
	check(err, 3, "Cannot read body")

	var unknownList []string
	err = json.Unmarshal(body, &unknownList)
	fmt.Println(string(body))
	check(err, 4, "Cannot parse response")

	for _, checksum := range unknownList {
		chunkBytes, err := index.ReadChunk(checksum)
		check(err, 5, "Cannot read chunk " + checksum)

		fmt.Println("Uploading", checksum)
		_, err = http.Post("http://localhost:8080/api/upload/" + checksum, "application/octet-stream", bytes.NewReader(chunkBytes))
		check(err, 6, "Bad response")

		index.DeleteChunk(checksum)
	}

	for _, checksum := range chunks {
		index.DeleteChunk(checksum)
	}
}
