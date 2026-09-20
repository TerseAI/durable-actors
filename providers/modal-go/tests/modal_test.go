package main

import "testing"

func TestSDKInitializesWithTheRustProvidersSanitizedEnvironment(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("MODAL_CONFIG_PATH", "")
	t.Setenv("MODAL_TOKEN_ID", "test-token")
	t.Setenv("MODAL_TOKEN_SECRET", "test-secret")
	api, closeClient, err := newModalAPI()
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()
	if api == nil {
		t.Fatal("no SDK client")
	}
}
