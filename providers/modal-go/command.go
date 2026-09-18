package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const maximumCommandBytes = 1024 * 1024

type apiFactory func() (modalAPI, func(), error)
type command struct {
	Operation string          `json:"operation"`
	Request   json.RawMessage `json:"request"`
}
type response struct {
	Status string `json:"status"`
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func runCommand(ctx context.Context, input io.Reader, output io.Writer, factory apiFactory, now func() time.Time) error {
	result, err := executeCommand(ctx, input, factory, now)
	reply := response{Status: "success", Result: result}
	if err != nil {
		reply = response{Status: "failure", Error: err.Error()}
	}
	document, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	if len(document) >= maximumCommandBytes {
		return fmt.Errorf("provider response is too large")
	}
	_, err = output.Write(append(document, '\n'))
	return err
}

func executeCommand(ctx context.Context, input io.Reader, factory apiFactory, now func() time.Time) (any, error) {
	started := now()
	cmd, err := readCommand(input)
	if err != nil {
		return nil, err
	}
	parsed := elapsed(started, now())
	api, closeClient, err := factory()
	if err != nil {
		return nil, err
	}
	defer closeClient()
	p := &provider{api: api, now: now, started: started, inputParsed: parsed, sdkLoaded: elapsed(started, now())}
	switch cmd.Operation {
	case "socket_credentials":
		var request socketRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.socketCredentials(ctx, request)
	case "ensure_replica":
		var request replicaRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.ensureReplica(ctx, request)
	case "ensure_host":
		var request ensureRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.ensureHost(ctx, request)
	case "warm_image":
		var request imageRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.warmImage(ctx, request)
	default:
		var request terminateRequest
		if err := json.Unmarshal(cmd.Request, &request); err != nil {
			return nil, err
		}
		return p.terminateHosts(ctx, request)
	}
}

func readCommand(input io.Reader) (command, error) {
	document, err := io.ReadAll(io.LimitReader(input, maximumCommandBytes+1))
	if err != nil {
		return command{}, err
	}
	if len(document) > maximumCommandBytes {
		return command{}, fmt.Errorf("provider command exceeds %d bytes", maximumCommandBytes)
	}
	var cmd command
	if err := json.Unmarshal(document, &cmd); err != nil {
		return cmd, err
	}
	switch cmd.Operation {
	case "ensure_host", "ensure_replica", "warm_image", "terminate_hosts", "socket_credentials":
		return cmd, nil
	default:
		return cmd, fmt.Errorf("unsupported sandbox operation")
	}
}

func elapsed(start, finish time.Time) int64 { return max(0, finish.Sub(start).Milliseconds()) }
