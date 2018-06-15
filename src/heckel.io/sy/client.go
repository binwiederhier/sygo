package main

import (
	"os"
	"fmt"
	"bufio"
	"io"
	"crypto/md5"
	"encoding/hex"
	"math/rand"
	"time"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"bytes"
	"path/filepath"
	"strings"
	"mime/multipart"
)

type Client struct {
	root string
	api string
	idx *index
	queue map[string]os.FileInfo
	queueSize int64
}

func NewClient(api string) Client {
	root := ".sy"
	index, err := NewIndex(root)
	check(err, 1, "Cannot open index")

	err = index.Load()
	check(err, 1, "Cannot load index")

	queue := make(map[string]os.FileInfo, 0)
	rand.Seed(time.Now().Unix())

	return Client{
		root: root,
		api: api,
		idx: index,
		queue: queue,
	}
}

func (client *Client) Index() {
	err := client.idx.Begin()
	check(err, 1, "Cannot start index transaction")

	//queue := make(chan string, 10)

	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && !strings.HasPrefix(path, client.root) {
			client.queueUpload(client.indexFile(path))
		}

		return nil
	})

	if len(client.queue) > 0 {
		client.removeKnownFromQueue()
		client.uploadQueue()
	}

	client.idx.Commit()
}

func (client *Client) indexFile(filename string) []string {
	file, err := os.Open(filename)
	check(err, 1, "Cannot open file")

	defer file.Close()

	client.idx.RemoveFile(filename)

	fileId := rand.Int()

	buffer := make([]byte, 4*1024*1024)
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

		client.idx.AddFileChunk(fileId, checksum, num)
		num++

		if !client.idx.Exists(checksum) {
			chunks = append(chunks, checksum)

			client.idx.AddChunk(checksum)
			client.idx.WriteChunk(checksum, buffer[:read])
		}
	}

	client.idx.AddFile(fileId, "", filename)

	return chunks
}

func (client *Client) retrieveUnknownChunks(knownChunks []string) []string {
	chunkListJson, err := json.Marshal(knownChunks)
	check(err, 1, "Cannot convert to JSON")

	diffUrl := fmt.Sprintf("%s/api/diff", client.api)
	responseStr, err := http.Post(diffUrl, "application/json", bytes.NewBuffer(chunkListJson))
	check(err, 2, "Cannot check chunks API")

	body, err := ioutil.ReadAll(responseStr.Body)
	check(err, 3, "Cannot read body")

	var unknownList []string
	err = json.Unmarshal(body, &unknownList)
	check(err, 4, "Cannot parse response")

	if len(unknownList) > 0 {
		fmt.Printf("%d unknown chunk(s)\n", len(unknownList))
	}

	return unknownList
}

func (client *Client) queueUpload(chunks []string) {
	for _, checksum := range chunks {
		filename := client.idx.GetChunkPath(checksum)
		info, err := os.Stat(filename)
		check(err, 6, "Cannot read chunk " + checksum)

		client.queue[checksum] = info
		client.queueSize += info.Size()

		if client.queueSize > 10 * 1024 * 1024 {
			client.removeKnownFromQueue()

			if client.queueSize > 10 * 1024 * 1024 {
				client.uploadQueue()
			}
		}
	}
}

func (client *Client) uploadQueue() {
	if len(client.queue) == 0 {
		return
	}

	fmt.Printf("Uploading queue of %d chunk(s) ...\n", len(client.queue))

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)

	for checksum, _ := range client.queue {
		part, err := writer.CreateFormFile(checksum, checksum)
		check(err, 1, "Cannot create form part for " + checksum)

		filename := client.idx.GetChunkPath(checksum)
		contents, err := ioutil.ReadFile(filename)
		check(err, 2, "Unable to read file " + filename)

		part.Write(contents)
	}

	err := writer.Close()
	check(err, 3, "Unable to close writer")
//fmt.Println("body", body)
	uploadUrl := fmt.Sprintf("%s/api/upload", client.api)
	request, err := http.NewRequest("POST", uploadUrl, body)
	check(err, 4, "Cannot create new POST request")

	request.Header.Set("Content-Type", writer.FormDataContentType())

	httpClient := http.Client{}
	response, err := httpClient.Do(request)
	check(err, 5, "Invalid or no response")

	ioutil.ReadAll(response.Body)
	// fmt.Println("->", string(s))

	for checksum, _ := range client.queue {
		client.idx.DeleteChunk(checksum)
	}

	client.queue = make(map[string]os.FileInfo, 0)
	client.queueSize = 0
}

func (client *Client) removeKnownFromQueue() {
	known := make([]string, 0)

	for checksum, _ := range client.queue {
		known = append(known, checksum)
	}

	unknown := client.retrieveUnknownChunks(known)
	newQueue := make(map[string]os.FileInfo, 0)
	var newQueueSize int64

	for _, checksum := range unknown {
		newQueue[checksum] = client.queue[checksum]
		newQueueSize += client.queue[checksum].Size()
	}

	client.queue = newQueue
	client.queueSize = newQueueSize
}