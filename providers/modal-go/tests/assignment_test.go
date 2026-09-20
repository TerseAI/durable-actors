package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDirectAssignmentAuthenticatesAndReturnsReadiness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/assign" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("unexpected assignment request")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var environment map[string]string
		if err := json.NewDecoder(r.Body).Decode(&environment); err != nil || environment["actor"] != "one" {
			t.Errorf("assignment missing actor: %v", err)
		}
		json.NewEncoder(w).Encode(map[string]any{"hostId": "host.v3.r1.one", "sessionId": "session", "route": "https://actor.test", "canonicalRegion": "north-america-east", "ownerEpoch": 42})
	}))
	defer server.Close()
	assigner := httpSpareAssigner{client: server.Client()}
	handle, err := assigner.Assign(context.Background(), spareHandle{ControlRoute: server.URL, ControlToken: "secret"}, map[string]string{"actor": "one"})
	if err != nil {
		t.Fatal(err)
	}
	if handle.OwnerEpoch != 42 || handle.SessionID != "session" {
		t.Fatal(handle)
	}
}

func TestDirectAssignmentRejectsFailedReadiness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	assigner := httpSpareAssigner{client: server.Client()}
	if _, err := assigner.Assign(context.Background(), spareHandle{ControlRoute: server.URL, ControlToken: "secret"}, nil); err == nil {
		t.Fatal("failed initialization reported ready")
	}
}
