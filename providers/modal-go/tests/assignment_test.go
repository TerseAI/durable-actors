package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDirectAssignmentAuthenticatesAndReturnsReadiness(t *testing.T) {
	client := &http.Client{Transport: assignmentTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.String() != "https://spare.w.modal.host/assign" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("unexpected assignment request")
		}
		var environment map[string]string
		if err := json.NewDecoder(r.Body).Decode(&environment); err != nil || environment["actor"] != "one" {
			t.Errorf("assignment missing actor: %v", err)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"sessionId":"session","ownerEpoch":42,"initialOutcome":{"requestId":"first","outcome":{"type":"completed","result":7}}}`))}, nil
	})}
	assigner := httpSpareAssigner{client: client}
	handle, err := assigner.Assign(context.Background(), spareHandle{ControlRoute: "https://spare.w.modal.host/", ControlToken: "secret"}, map[string]string{"actor": "one"})
	if err != nil {
		t.Fatal(err)
	}
	if handle.OwnerEpoch != 42 || handle.SessionID != "session" || string(handle.InitialOutcome) != `{"requestId":"first","outcome":{"type":"completed","result":7}}` {
		t.Fatal(handle)
	}
}

func TestDirectAssignmentPreservesWhetherExecutionWasRejected(t *testing.T) {
	for _, tc := range []struct {
		status      int
		body        string
		notExecuted bool
	}{
		{http.StatusServiceUnavailable, "", false},
		{http.StatusPreconditionFailed, `{"type":"not_executed"}`, true},
		{http.StatusPreconditionFailed, `{"type":"unknown"}`, false},
	} {
		assigner := httpSpareAssigner{client: &http.Client{Transport: assignmentTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})}}
		_, failure := assigner.Assign(context.Background(), spareHandle{ControlRoute: "https://spare.r5.modal.host", ControlToken: "secret"}, nil)
		if failure == nil {
			t.Fatal("failed initialization reported ready")
		}
		var output bytes.Buffer
		if err := writeReply(&output, nil, failure); err != nil {
			t.Fatal(err)
		}
		var reply struct{ NotExecuted bool }
		if err := json.Unmarshal(output.Bytes(), &reply); err != nil {
			t.Fatal(err)
		}
		if reply.NotExecuted != tc.notExecuted {
			t.Fatalf("incorrect execution certainty: %s", output.String())
		}
	}
}

func TestDirectAssignmentRejectsUntrustedEndpointsBeforeSendingCredentials(t *testing.T) {
	assigner := httpSpareAssigner{client: &http.Client{Transport: assignmentTransport(func(*http.Request) (*http.Response, error) {
		t.Error("sent credentials to an untrusted endpoint")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}}
	for _, route := range []string{"http://spare.w.modal.host", "https://169.254.169.254", "https://spare.modal.host.attacker.test", "https://spare.w.modal.host@attacker.test", "https://spare.w.modal.host:8080", "https://spare.w.modal.host/redirect?to=elsewhere"} {
		t.Run(route, func(t *testing.T) {
			if _, err := assigner.Assign(context.Background(), spareHandle{ControlRoute: route, ControlToken: "secret"}, nil); err == nil {
				t.Fatal("accepted untrusted assignment endpoint")
			}
		})
	}
}

type assignmentTransport func(*http.Request) (*http.Response, error)

func (f assignmentTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWarmOpensAnUnauthenticatedConnectionToTheSpare(t *testing.T) {
	var requests []string
	assigner := httpSpareAssigner{client: &http.Client{Transport: assignmentTransport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "" {
			t.Error("warmup sent credentials")
		}
		requests = append(requests, r.Method+" "+r.URL.String())
		return &http.Response{StatusCode: http.StatusMethodNotAllowed, Body: http.NoBody}, nil
	})}}
	assigner.Warm(context.Background(), spareHandle{ControlRoute: "https://spare.w.modal.host/", ControlToken: "secret"})
	assigner.Warm(context.Background(), spareHandle{ControlRoute: "https://169.254.169.254", ControlToken: "secret"})
	if len(requests) != 1 || requests[0] != "GET https://spare.w.modal.host/assign" {
		t.Fatal(requests)
	}
}
