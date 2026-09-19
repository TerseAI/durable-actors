package main

import (
	"context"
	"os"
	"testing"
	"time"

	modal "github.com/modal-labs/modal-client/go"
)

func TestNamedSecretInSandbox(t *testing.T) {
	name := os.Getenv("DURABLE_OBJECT_TEST_MODAL_SECRET")
	if name == "" {
		t.Skip("requires a throwaway named Modal secret containing DO_QA_SECRET=injected-qa")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	api, closeClient, err := newModalAPI()
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()
	sdk := api.(*sdkAPI)
	app, err := sdk.client.Apps.FromName(ctx, "durable-object-secret-qa", &modal.AppFromNameParams{CreateIfMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	secret, err := api.Secret(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := api.Create(ctx, app, sdk.client.Images.FromRegistry("alpine:3.21", nil), &modal.SandboxCreateParams{
		Command: []string{"sh", "-c", `test "$DO_QA_SECRET" = injected-qa`},
		Secrets: []*modal.Secret{secret}, Timeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Detach()
	defer terminateForCleanup(sb)
	code, err := sb.Wait(ctx)
	if err != nil || code != 0 {
		t.Fatalf("secret injection failed: exit=%d error=%v", code, err)
	}
}
