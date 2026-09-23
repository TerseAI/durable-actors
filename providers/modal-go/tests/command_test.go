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

func TestDeploymentBuildPublishesAfterCompilationAndAlwaysStopsTheBuilder(t *testing.T) {
	for _, failure := range []string{"", "compile", "publish"} {
		sb := &fakeSandbox{}
		if failure == "compile" {
			sb.buildErr = errors.New("invalid actor source")
		}
		if failure == "publish" {
			sb.snapshotErr = errors.New("snapshot failed")
		}
		api := &fakeAPI{created: sb}
		factory := func() (modalAPI, func(), error) { return api, func() {}, nil }
		var output bytes.Buffer
		input := strings.NewReader(`{"operation":"build_code","request":{"imageRef":"im-customer","workingDirectory":"/project","actorEntrypoint":"src/actors.ts","canonicalRegion":"north-america-east"}}`)
		if err := runCommand(context.Background(), input, &output, factory, time.Now); err != nil {
			t.Fatal(err)
		}
		if api.params.MemoryMiB != 4096 || api.params.MemoryLimitMiB != 4096 {
			t.Fatalf("compiler needs a dedicated 4 GiB sandbox: %+v", api.params)
		}
		if len(sb.calls) < 3 || sb.calls[0] != "build:/project:src/actors.ts" || sb.calls[len(sb.calls)-2] != "terminate" || sb.calls[len(sb.calls)-1] != "detach" {
			t.Fatalf("build lifecycle: %v, %s", sb.calls, output.String())
		}
		if failure == "compile" {
			if len(sb.calls) != 3 || !strings.Contains(output.String(), "invalid actor source") {
				t.Fatal(output.String(), sb.calls)
			}
		} else if failure == "publish" {
			if !strings.Contains(output.String(), "snapshot failed") {
				t.Fatal(output.String())
			}
		} else if !strings.Contains(output.String(), `"codeSnapshot":"im-code"`) || sb.calls[1] != "snapshot" {
			t.Fatal(output.String(), sb.calls)
		}
	}
}

func TestBuildReplyCanCarryAContractLargerThanTheCommandLimit(t *testing.T) {
	document, _ := json.Marshal(map[string]any{"version": 1, "actors": []any{}, "padding": strings.Repeat("a", maximumCommandBytes)})
	sb := &fakeSandbox{contract: document}
	factory := func() (modalAPI, func(), error) { return &fakeAPI{created: sb}, func() {}, nil }
	var output bytes.Buffer
	input := strings.NewReader(`{"operation":"build_code","request":{"imageRef":"im-customer","workingDirectory":"/project","actorEntrypoint":"src/actors.ts","canonicalRegion":"north-america-east"}}`)
	if err := runCommand(context.Background(), input, &output, factory, time.Now); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Status string
		Result struct{ Contract json.RawMessage }
	}
	if err := json.Unmarshal(output.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Status != "success" || !bytes.Equal(reply.Result.Contract, document) {
		t.Fatal("build contract was lost")
	}
}
