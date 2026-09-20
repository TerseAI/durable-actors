package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCommandRejectsInvalidInputBeforeConnecting(t *testing.T) {
	for _, input := range []string{`{`, `{} {}`, `{"operation":"unknown","request":{}}`, `{"operation":"warm_image","request":{}}`, strings.Repeat("x", 1024*1024+1)} {
		t.Run(input[:min(len(input), 40)], func(t *testing.T) {
			var output bytes.Buffer
			factory := func() (modalAPI, func(), error) { t.Fatal("unexpected SDK initialization"); return nil, nil, nil }
			if err := runCommand(context.Background(), strings.NewReader(input), &output, factory, time.Now); err != nil {
				t.Fatal(err)
			}
			var reply struct {
				Status string
				Error  string
			}
			if err := json.Unmarshal(output.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			if reply.Status != "failure" || reply.Error == "" {
				t.Fatalf("unexpected reply: %s", output.String())
			}
		})
	}
}

func TestCommandReportsSDKInitializationFailure(t *testing.T) {
	var output bytes.Buffer
	factory := func() (modalAPI, func(), error) { return nil, nil, errors.New("SDK unavailable") }
	err := runCommand(context.Background(), strings.NewReader(`{"operation":"create_spare","request":{}}`), &output, factory, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"status":"failure"`) || !strings.Contains(output.String(), "SDK unavailable") {
		t.Fatal(output.String())
	}
}

func TestCommandReturnsOneCamelCaseReplyAndClosesSDK(t *testing.T) {
	api := &fakeAPI{created: &fakeSandbox{controlRoute: assignmentServer(t).URL}}
	closed := false
	factory := func() (modalAPI, func(), error) { return api, func() { closed = true }, nil }
	request, err := json.Marshal(testRequest())
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(command{Operation: "ensure_host", Request: request})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runCommand(context.Background(), bytes.NewReader(input), &output, factory, time.Now); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Status string
		Result hostHandle
	}
	if err := json.Unmarshal(output.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if !closed || reply.Status != "success" || reply.Result.HostID != testRequest().HostID {
		t.Fatal(output.String())
	}
	if !strings.Contains(output.String(), `"sdkLoadedAtMs":`) || strings.Count(output.String(), "\n") != 1 {
		t.Fatal(output.String())
	}
}

func TestRetireCommandReturnsResultAfterTerminatingSandbox(t *testing.T) {
	sb := &fakeSandbox{}
	api := &fakeAPI{found: sb}
	factory := func() (modalAPI, func(), error) { return api, func() {}, nil }
	var output bytes.Buffer
	input := strings.NewReader(`{"operation":"retire_spare","request":{"resourceId":"sb-test"}}`)
	if err := runCommand(context.Background(), input, &output, factory, time.Now); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Status string
		Result json.RawMessage
	}
	if err := json.Unmarshal(output.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Status != "success" || len(reply.Result) == 0 {
		t.Fatalf("Rust requires a result in successful provider replies: %s", output.String())
	}
	if len(sb.calls) != 2 || sb.calls[0] != "terminate" || sb.calls[1] != "detach" {
		t.Fatalf("sandbox was not retired: %v", sb.calls)
	}
}
