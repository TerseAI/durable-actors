package main

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

type ensureRequest struct {
	RuntimeConfig         string   `json:"runtimeConfig"`
	NamespaceID           string   `json:"namespaceId"`
	CodeRevision          string   `json:"codeRevision"`
	CanonicalRegion       string   `json:"canonicalRegion"`
	HostID                string   `json:"hostId"`
	SessionID             string   `json:"sessionId"`
	HostToken             string   `json:"hostToken"`
	JWTPublicKeys         string   `json:"jwtPublicKeys"`
	ControlPlaneURL       string   `json:"controlPlaneUrl"`
	JWTIssuer             string   `json:"jwtIssuer"`
	InvocationJWTAudience string   `json:"invocationJwtAudience"`
	ImageRef              string   `json:"imageRef"`
	WorkingDirectory      string   `json:"workingDirectory"`
	ActorEntrypoint       string   `json:"actorEntrypoint"`
	SecretRefs            []string `json:"secretRefs"`
	SocketGatewayURL      string   `json:"socketGatewayUrl"`
	ActorIdleTimeoutMS    int64    `json:"actorIdleTimeoutMs"`
	HostIdleTimeoutMS     int64    `json:"hostIdleTimeoutMs"`
}
type imageRequest struct {
	NamespaceID     string `json:"namespaceId"`
	CodeRevision    string `json:"codeRevision"`
	CanonicalRegion string `json:"canonicalRegion"`
	ImageRef        string `json:"imageRef"`
}
type terminateRequest struct {
	NamespaceID      string   `json:"namespaceId"`
	CodeRevision     string   `json:"codeRevision"`
	CanonicalRegions []string `json:"canonicalRegions"`
}
type hostHandle struct {
	HostID          string        `json:"hostId"`
	Route           string        `json:"route"`
	CanonicalRegion string        `json:"canonicalRegion"`
	Provisioning    *provisioning `json:"provisioning,omitempty"`
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
type imageWarmup struct {
	Provider   string `json:"provider"`
	ResourceID string `json:"resourceId"`
	TotalMS    int64  `json:"totalMs"`
}
type hostTermination struct {
	Provider    string   `json:"provider"`
	ResourceIDs []string `json:"resourceIds"`
}

func validateEnsure(request ensureRequest) error {
	if request.NamespaceID == "" || request.CodeRevision == "" || request.ImageRef == "" || !strings.HasPrefix(request.HostID, "host.v2."+request.NamespaceID+":") {
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

func resourceName(namespace, revision, region string) string {
	digest := sha256.Sum256([]byte(namespace + "\x00" + revision + "\x00" + region))
	return fmt.Sprintf("do-host-v2-%x", digest[:16])
}

func hostEnvironment(r ensureRequest) map[string]string {
	env := map[string]string{
		"DURABLE_OBJECT_PROCESS_ROLE": "host", "DURABLE_OBJECT_HOST_TOKEN": r.HostToken,
		"DURABLE_OBJECT_JWT_PUBLIC_KEYS": r.JWTPublicKeys, "DURABLE_OBJECT_NAMESPACE_ID": r.NamespaceID,
		"DURABLE_OBJECT_CONTROL_PLANE_URL": r.ControlPlaneURL, "DURABLE_OBJECT_JWT_ISSUER": r.JWTIssuer,
		"DURABLE_OBJECT_INVOKE_JWT_AUDIENCE": r.InvocationJWTAudience, "DURABLE_OBJECT_HOST_ID": r.HostID,
		"DURABLE_OBJECT_SESSION_ID": r.SessionID, "DURABLE_OBJECT_REGION": r.CanonicalRegion,
		"DURABLE_OBJECT_CODE_REVISION": r.CodeRevision, "DURABLE_OBJECT_EXECUTOR_SOCKET": "/tmp/durable-object-executor.sock",
		"DURABLE_OBJECT_HOST_READY_FILE": readyFile, "DURABLE_OBJECT_HOST_METADATA_FILE": metadataFile,
		"DURABLE_OBJECT_HOST_BIND": "0.0.0.0:7101", "DURABLE_OBJECT_HOST_PUBLIC_ROUTE_FILE": routeFile,
		"DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS": fmt.Sprint(r.ActorIdleTimeoutMS), "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS": fmt.Sprint(r.HostIdleTimeoutMS),
	}
	if r.RuntimeConfig != "" {
		env["DURABLE_OBJECT_RUNTIME_CONFIG"] = r.RuntimeConfig
	}
	if r.ActorEntrypoint != "" {
		env["DURABLE_OBJECT_ENTRYPOINT"] = r.ActorEntrypoint
	}
	if r.SocketGatewayURL != "" {
		env["DURABLE_OBJECT_SOCKET_GATEWAY_URL"] = r.SocketGatewayURL
	}
	return env
}
