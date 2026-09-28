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

func TestPythonBuildCommand(t *testing.T) {
	command := buildCodeCommand("/project", "actors.py")
	if command[0] != "python3" || command[1] != "-m" || command[2] != "little_actors.build" {
		t.Fatalf("unexpected Python build command: %v", command)
	}
}
