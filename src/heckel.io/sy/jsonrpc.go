package main

import (
	"math/rand"
	"encoding/json"
	"strconv"
)

type JsonRpc struct {
	Jsonrpc string `json:"jsonrpc"`
	Id string `json:"id"`
}

type JsonRpcRequest struct {
	JsonRpc
	Method  string `json:"method"`
	Params map[string]interface{} `json:"params"`
}

type JsonRpcResponse struct {
	JsonRpc
	Result map[string]interface{} `json:"result"`
	Error string `json:"error"`
}

func NewJsonRpcRequest(method string, params map[string]interface{}) JsonRpcRequest {
	request := JsonRpcRequest{}

	request.Jsonrpc = "2.0"
	request.Id = strconv.Itoa(rand.Int())
	request.Method = method
	request.Params = params

	return request
}

func ParseJsonRpcRequest(bytes []byte) (JsonRpcRequest, error) {
	var request JsonRpcRequest
	err := json.Unmarshal(bytes, &request)

	if err != nil || request.Jsonrpc != "2.0" || request.Method == "" {
		return request, err
	}

	return request, nil
}


func ParseJsonRpcResponse(bytes []byte) (JsonRpcResponse, error) {
	var response JsonRpcResponse
	err := json.Unmarshal(bytes, &response)

	if err != nil || response.Jsonrpc != "2.0" {
		return response, err
	}

	return response, nil
}
