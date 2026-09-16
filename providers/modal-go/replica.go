package main

import (
	"context"
	"crypto/sha256"
	"fmt"
)

type replicaRequest struct {
	InstallationID  string `json:"installationId"`
	Slot            int    `json:"slot"`
	CanonicalRegion string `json:"canonicalRegion"`
	ImageRef        string `json:"imageRef"`
	HostID          string `json:"hostId"`
	Secret          string `json:"secret"`
	ControlPlaneURL string `json:"controlPlaneUrl"`
}

func (p *provider) ensureReplica(ctx context.Context, r replicaRequest) (hostHandle, error) {
	if r.InstallationID == "" || r.Slot < 0 || r.Slot >= 8 || r.ImageRef == "" || r.HostID == "" || r.Secret == "" {
		return hostHandle{}, fmt.Errorf("invalid replica identity or configuration")
	}
	request := ensureRequest{HostID: r.HostID, CanonicalRegion: r.CanonicalRegion, ImageRef: r.ImageRef}
	params, err := hostParams(request)
	if err != nil {
		return hostHandle{}, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", r.InstallationID, r.CanonicalRegion, r.Slot)))
	params.Name = fmt.Sprintf("do-replica-%x", digest[:16])
	params.Workdir = "/tmp"
	params.Env = map[string]string{
		"DURABLE_OBJECT_PROCESS_ROLE":           "replica",
		"DURABLE_OBJECT_REPLICA_SECRET":         r.Secret,
		"DURABLE_OBJECT_HOST_ID":                r.HostID,
		"DURABLE_OBJECT_REGION":                 r.CanonicalRegion,
		"DURABLE_OBJECT_HOST_BIND":              "0.0.0.0:7101",
		"DURABLE_OBJECT_HOST_PUBLIC_ROUTE_FILE": routeFile,
		"DURABLE_OBJECT_HOST_METADATA_FILE":     metadataFile,
		"DURABLE_OBJECT_HOST_READY_FILE":        readyFile,
		"DURABLE_OBJECT_REPLICA_DATA":           "/tmp/durable-object-replica/state.db",
	}
	return p.ensureSandbox(ctx, request, params)
}
