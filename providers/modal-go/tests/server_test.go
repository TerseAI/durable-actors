package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProviderServerRetainsTheSpareAcrossCommands(t *testing.T) {
	sb := &fakeSandbox{}
	api := &fakeAPI{created: sb, findErr: errors.New("unexpected lookup")}
	client, _ := startProviderServer(t, func() (modalAPI, func(), error) { return api, func() {}, nil })
	call := func(operation string, request any, result any) {
		t.Helper()
		document, _ := json.Marshal(map[string]any{"operation": operation, "request": request})
		reply, err := client.Post("http://provider/", "application/json", bytes.NewReader(document))
		if err != nil {
			t.Fatal(err)
		}
		defer reply.Body.Close()
		var value struct {
			Status string
			Error  string
			Result json.RawMessage
		}
		if err := json.NewDecoder(reply.Body).Decode(&value); err != nil {
			t.Fatal(err)
		}
		if value.Status != "success" {
			t.Fatal(value.Error)
		}
		if err := json.Unmarshal(value.Result, result); err != nil {
			t.Fatal(err)
		}
	}
	var spare spareHandle
	call("create_spare", spareRequest{Kind: "actor", Name: "do-spare-test", ImageRef: "im-runtime", CanonicalRegion: "north-america-east", Resources: resourceLimits{CPUMillis: 1000, MemoryMiB: 1024}}, &spare)
	call("retire_spare", spare, &struct{}{})
	if sb.calls[len(sb.calls)-2] != "terminate" || sb.calls[len(sb.calls)-1] != "detach" {
		t.Fatal("retained spare was not retired", sb.calls)
	}
}

type cancellableAPI struct {
	modalAPI
	started chan struct{}
	stopped chan struct{}
}

func (a *cancellableAPI) ByID(ctx context.Context, id string) (sandbox, error) {
	if id == "blocked" {
		close(a.started)
		<-ctx.Done()
		close(a.stopped)
		return nil, ctx.Err()
	}
	return &fakeSandbox{}, nil
}

func TestProviderServerSharesClientAndCancelsOnlyTheDisconnectedRequest(t *testing.T) {
	api := &cancellableAPI{modalAPI: &fakeAPI{}, started: make(chan struct{}), stopped: make(chan struct{})}
	var opened, closed atomic.Int32
	factory := func() (modalAPI, func(), error) { opened.Add(1); return api, func() { closed.Add(1) }, nil }
	client, shutdown := startProviderServer(t, factory)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://provider/", strings.NewReader(`{"operation":"retire_spare","request":{"resourceId":"blocked"}}`))
	done := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	waitSignal(t, api.started)
	assertProviderReply(t, client)
	cancel()
	waitSignal(t, api.stopped)
	if err := <-done; err == nil {
		t.Fatal("cancelled request succeeded")
	}
	assertProviderReply(t, client)
	if opened.Load() != 1 || closed.Load() != 0 {
		t.Fatal("client was not shared")
	}
	shutdown()
	if closed.Load() != 1 {
		t.Fatal("provider did not close its client")
	}
}

func TestProviderServerShutdownCancelsActiveRequests(t *testing.T) {
	api := &cancellableAPI{modalAPI: &fakeAPI{}, started: make(chan struct{}), stopped: make(chan struct{})}
	client, shutdown := startProviderServer(t, func() (modalAPI, func(), error) { return api, func() {}, nil })
	done := make(chan struct{})
	go func() {
		defer close(done)
		response, _ := client.Post("http://provider/", "application/json", strings.NewReader(`{"operation":"retire_spare","request":{"resourceId":"blocked"}}`))
		if response != nil {
			response.Body.Close()
		}
	}()
	waitSignal(t, api.started)
	shutdown()
	waitSignal(t, api.stopped)
	waitSignal(t, done)
}

func startProviderServer(t *testing.T, factory apiFactory) (*http.Client, func()) {
	t.Helper()
	directory, err := os.MkdirTemp("", "da-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	ctx, cancel := context.WithCancel(context.Background())
	input, output := io.Pipe()
	done := make(chan error, 1)
	socket := filepath.Join(directory, "socket")
	go func() { defer output.Close(); done <- serveProvider(ctx, socket, output, factory, time.Now) }()
	line, err := bufio.NewReader(input).ReadString('\n')
	input.Close()
	if err != nil || line != "{\"protocol\":1}\n" {
		cancel()
		t.Fatalf("readiness: %q %v", line, err)
	}
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("provider socket is not private")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	stopped := false
	shutdown := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("provider did not stop")
		}
		transport.CloseIdleConnections()
	}
	t.Cleanup(shutdown)
	return client, shutdown
}

func assertProviderReply(t *testing.T, client *http.Client) {
	t.Helper()
	reply, err := client.Post("http://provider/", "application/json", strings.NewReader(`{"operation":"retire_spare","request":{"resourceId":"healthy"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer reply.Body.Close()
	var result response
	if err := json.NewDecoder(reply.Body).Decode(&result); err != nil || result.Status != "success" {
		t.Fatalf("healthy command failed: %+v %v", result, err)
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("operation timed out")
	}
}
