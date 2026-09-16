package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestReplicaHostsUseSeparateIdentitiesWithoutActorExecutionOrSecrets(t *testing.T) {
	api := &fakeAPI{created: &fakeSandbox{}}
	request := replicaRequest{InstallationID: "installation", Slot: 1, CanonicalRegion: "north-america-east", ImageRef: "im-runtime", HostID: "replica-one", Secret: "storage-only", ControlPlaneURL: "https://control.test"}
	handle, err := newTestProvider(api).ensureReplica(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if handle.HostID != request.HostID || api.params.Env["DURABLE_OBJECT_PROCESS_ROLE"] != "replica" {
		t.Fatal("replica process was not provisioned")
	}
	if _, exists := api.params.Env["DURABLE_OBJECT_HOST_TOKEN"]; exists {
		t.Fatal("actor authority leaked to replica")
	}
	if len(api.params.Secrets) != 0 || api.params.IdleTimeout != 0 || api.params.Timeout != 24*time.Hour {
		t.Fatal("replicas must outlive actor deployments and idle shutdown")
	}
	first := api.params.Name
	request.Slot++
	if _, err := newTestProvider(api).ensureReplica(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if first == api.params.Name || !strings.HasPrefix(first, "do-replica-") {
		t.Fatal("replica slots must provision distinct sandboxes")
	}
}
