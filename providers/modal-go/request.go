package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type ensureRequest struct {
	ActorIsNew            bool            `json:"actorIsNew"`
	Actor                 json.RawMessage `json:"actor"`
	CodeSnapshot          string          `json:"codeSnapshot"`
	Spare                 *spareHandle    `json:"spare"`
	Resources             resourceLimits  `json:"resources"`
	SocketJWTAudience     string          `json:"socketJwtAudience"`
	RuntimeConfig         string          `json:"runtimeConfig"`
	CodeRevision          string          `json:"codeRevision"`
	CanonicalRegion       string          `json:"canonicalRegion"`
	HostID                string          `json:"hostId"`
	SessionID             string          `json:"sessionId"`
	HostToken             string          `json:"hostToken"`
	JWTPublicKeys         string          `json:"jwtPublicKeys"`
	ControlPlaneURL       string          `json:"controlPlaneUrl"`
	JWTIssuer             string          `json:"jwtIssuer"`
	InvocationJWTAudience string          `json:"invocationJwtAudience"`
	ImageRef              string          `json:"imageRef"`
	WorkingDirectory      string          `json:"workingDirectory"`
	ActorEntrypoint       string          `json:"actorEntrypoint"`
	SecretRefs            []string        `json:"secretRefs"`
	ActorIdleTimeoutMS    int64           `json:"actorIdleTimeoutMs"`
	HostIdleTimeoutMS     int64           `json:"hostIdleTimeoutMs"`
}
type hostHandle struct {
	Lease           *activationLease `json:"lease,omitempty"`
	SessionID       string           `json:"sessionId,omitempty"`
	OwnerEpoch      uint64           `json:"ownerEpoch,omitempty"`
	HostID          string           `json:"hostId"`
	Route           string           `json:"route"`
	CanonicalRegion string           `json:"canonicalRegion"`
	Provisioning    *provisioning    `json:"provisioning,omitempty"`
}
type activationLease struct {
	ID          string `json:"id"`
	SessionID   string `json:"session_id"`
	Route       string `json:"route"`
	ExpiresAtMS uint64 `json:"expires_at_ms"`
}

type provisioning struct {
	Provider              string `json:"provider"`
	ResourceID            string `json:"resourceId"`
	Reused                bool   `json:"reused"`
	StartedAtMS           int64  `json:"startedAtMs"`
	InputParsedAtMS       int64  `json:"inputParsedAtMs"`
	SDKLoadedAtMS         int64  `json:"sdkLoadedAtMs"`
	ResourcesResolvedAtMS int64  `json:"resourcesResolvedAtMs"`
	SandboxScheduledAtMS  int64  `json:"sandboxScheduledAtMs"`
	HostReadyObservedAtMS int64  `json:"hostReadyObservedAtMs"`
	RouteReadAtMS         int64  `json:"routeReadAtMs"`
	CompletedAtMS         int64  `json:"completedAtMs"`
}

func validateEnsure(request ensureRequest) error {
	if request.SessionID == "" || request.CodeRevision == "" || request.ImageRef == "" || !strings.HasPrefix(request.HostID, "host.v3."+request.CodeRevision+".") {
		return fmt.Errorf("invalid host identity or image")
	}
	for _, timeout := range []int64{request.ActorIdleTimeoutMS, request.HostIdleTimeoutMS} {
		if timeout <= 0 || timeout > 86400000 {
			return fmt.Errorf("actor or host idle timeout is invalid")
		}
	}
	return nil
}

func modalRegion(region string) (string, error) {
	regions := map[string]string{"north-america-east": "us-east", "north-america-central": "us-central", "north-america-south": "us-south", "north-america-west": "us-west", "europe-west": "eu-west", "asia-southeast": "ap-southeast"}
	if placement, ok := regions[region]; ok {
		return placement, nil
	}
	return "", fmt.Errorf("canonical region %q has no Modal placement", region)
}

func modalCloud(region string) string {
	if region == "north-america-east" {
		return ""
	}
	return "gcp"
}

func hostEnvironment(r ensureRequest) map[string]string {
	env := map[string]string{
		"DURABLE_OBJECT_PROCESS_ROLE": "host", "DURABLE_OBJECT_HOST_TOKEN": r.HostToken,
		"DURABLE_OBJECT_JWT_PUBLIC_KEYS":   r.JWTPublicKeys,
		"DURABLE_OBJECT_CONTROL_PLANE_URL": r.ControlPlaneURL, "DURABLE_OBJECT_JWT_ISSUER": r.JWTIssuer,
		"DURABLE_OBJECT_INVOKE_JWT_AUDIENCE": r.InvocationJWTAudience, "DURABLE_OBJECT_SOCKET_JWT_AUDIENCE": r.SocketJWTAudience, "DURABLE_OBJECT_HOST_ID": r.HostID,
		"DURABLE_OBJECT_SESSION_ID": r.SessionID, "DURABLE_OBJECT_REGION": r.CanonicalRegion,
		"DURABLE_OBJECT_CODE_REVISION": r.CodeRevision, "DURABLE_OBJECT_EXECUTOR_SOCKET": "/tmp/durable-object-executor.sock",
		"DURABLE_OBJECT_HOST_READY_FILE": readyFile, "DURABLE_OBJECT_HOST_METADATA_FILE": metadataFile,
		"DURABLE_OBJECT_HOST_BIND":             "0.0.0.0:7101",
		"DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS": fmt.Sprint(r.ActorIdleTimeoutMS), "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS": fmt.Sprint(r.HostIdleTimeoutMS),
	}
	env["DURABLE_OBJECT_ACTOR"] = string(r.Actor)
	env["DURABLE_OBJECT_ACTOR_IS_NEW"] = fmt.Sprint(r.ActorIsNew)
	if r.RuntimeConfig != "" {
		env["DURABLE_OBJECT_RUNTIME_CONFIG"] = r.RuntimeConfig
	}
	if r.ActorEntrypoint != "" {
		env["DURABLE_OBJECT_ENTRYPOINT"] = r.ActorEntrypoint
	}
	return env
}

type socketRequest struct {
	ResourceID      string `json:"resourceId"`
	CanonicalRegion string `json:"canonicalRegion"`
	HostID          string `json:"hostId"`
	SessionID       string `json:"sessionId"`
}

type socketCredentials struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}
