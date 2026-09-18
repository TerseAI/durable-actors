package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	modal "github.com/modal-labs/modal-client/go"
)

const (
	appName      = "durable-object-hosts"
	routeFile    = "/tmp/durable-object-route"
	metadataFile = "/tmp/durable-object-host.json"
	readyFile    = "/tmp/durable-object-ready"
	stderrFile   = "/tmp/durable-object-host.stderr"
)

type modalAPI interface {
	Resolve(context.Context, string) (*modal.App, *modal.Image, error)
	Secret(context.Context, string) (*modal.Secret, error)
	Create(context.Context, *modal.App, *modal.Image, *modal.SandboxCreateParams) (sandbox, error)
	Find(context.Context, string) (sandbox, error)
}

type sandbox interface {
	ID() string
	Route(context.Context) (string, error)
	Connect(context.Context) (socketCredentials, error)
	WriteFile(context.Context, string, string) error
	Ready(context.Context) error
	Metadata(context.Context) ([]byte, error)
	FailureDetail(context.Context) string
	Poll(context.Context) (*int, error)
	Wait(context.Context) (int, error)
	Terminate(context.Context) error
	Detach()
}

type provider struct {
	api                    modalAPI
	now                    func() time.Time
	started                time.Time
	inputParsed, sdkLoaded int64
}

func (p *provider) ensureHost(ctx context.Context, request ensureRequest) (hostHandle, error) {
	if err := validateEnsure(request); err != nil {
		return hostHandle{}, err
	}
	params, err := hostParams(request)
	if err != nil {
		return hostHandle{}, err
	}
	return p.ensureSandbox(ctx, request, params)
}

func (p *provider) ensureSandbox(ctx context.Context, request ensureRequest, params *modal.SandboxCreateParams) (hostHandle, error) {
	phases := &provisioning{Provider: "modal", StartedAtMS: p.elapsed(), InputParsedAtMS: p.inputParsed, SDKLoadedAtMS: p.sdkLoaded}
	app, image, err := p.api.Resolve(ctx, request.ImageRef)
	if err != nil {
		return hostHandle{}, err
	}
	phases.ResourcesResolvedAtMS = p.elapsed()
	for _, reference := range request.SecretRefs {
		secret, err := p.api.Secret(ctx, reference)
		if err != nil {
			return hostHandle{}, err
		}
		params.Secrets = append(params.Secrets, secret)
	}
	for attempt := 0; attempt < 2; attempt++ {
		sb, reused, err := p.acquire(ctx, app, image, params)
		if err != nil {
			return hostHandle{}, err
		}
		phases.SandboxScheduledAtMS = p.elapsed()
		handle, retry, err := p.finishHost(ctx, sb, reused, request, phases)
		if err != nil || !retry {
			return handle, err
		}
	}
	return hostHandle{}, fmt.Errorf("concurrent Modal V2 host could not be reused")
}

func (p *provider) socketCredentials(ctx context.Context, request socketRequest) (socketCredentials, error) {
	if request.CodeRevision == "" || request.HostID == "" || request.SessionID == "" {
		return socketCredentials{}, fmt.Errorf("invalid socket host identity")
	}
	if _, err := modalRegion(request.CanonicalRegion); err != nil {
		return socketCredentials{}, err
	}
	sb, err := p.api.Find(ctx, resourceName(request.CodeRevision, request.CanonicalRegion))
	if err != nil {
		return socketCredentials{}, err
	}
	defer sb.Detach()
	document, err := sb.Metadata(ctx)
	if err != nil {
		return socketCredentials{}, err
	}
	var metadata struct {
		HostID          string `json:"hostId"`
		SessionID       string `json:"sessionId"`
		CanonicalRegion string `json:"canonicalRegion"`
	}
	if err := json.Unmarshal(document, &metadata); err != nil {
		return socketCredentials{}, err
	}
	if metadata.HostID != request.HostID || metadata.SessionID != request.SessionID || metadata.CanonicalRegion != request.CanonicalRegion {
		return socketCredentials{}, fmt.Errorf("socket host session was replaced; resolve a new target")
	}
	return sb.Connect(ctx)
}

func (p *provider) warmImage(ctx context.Context, request imageRequest) (imageWarmup, error) {
	if request.CodeRevision == "" || request.ImageRef == "" {
		return imageWarmup{}, fmt.Errorf("image warmup request is invalid")
	}
	region, err := modalRegion(request.CanonicalRegion)
	if err != nil {
		return imageWarmup{}, err
	}
	app, image, err := p.api.Resolve(ctx, request.ImageRef)
	if err != nil {
		return imageWarmup{}, err
	}
	sb, err := p.api.Create(ctx, app, image, &modal.SandboxCreateParams{Command: []string{"true"}, Timeout: 2 * time.Minute, Regions: []string{region}, Cloud: modalCloud(request.CanonicalRegion)})
	if err != nil {
		return imageWarmup{}, err
	}
	defer sb.Detach()
	defer terminateForCleanup(sb)
	exitCode, err := sb.Wait(ctx)
	if err != nil {
		return imageWarmup{}, err
	}
	if exitCode != 0 {
		return imageWarmup{}, fmt.Errorf("Modal image warmup exited with status %d", exitCode)
	}
	return imageWarmup{Provider: "modal", ResourceID: sb.ID(), TotalMS: p.elapsed()}, nil
}

func (p *provider) terminateHosts(ctx context.Context, request terminateRequest) (hostTermination, error) {
	result := hostTermination{Provider: "modal", ResourceIDs: []string{}}
	if request.CodeRevision == "" || len(request.CanonicalRegions) == 0 {
		return result, fmt.Errorf("host termination request is invalid")
	}
	for _, region := range request.CanonicalRegions {
		if _, err := modalRegion(region); err != nil {
			return result, err
		}
	}
	for _, region := range request.CanonicalRegions {
		id, err := p.terminateNamed(ctx, resourceName(request.CodeRevision, region))
		if err != nil {
			return result, err
		}
		if id != "" {
			result.ResourceIDs = append(result.ResourceIDs, id)
		}
	}
	return result, nil
}

func (p *provider) acquire(ctx context.Context, app *modal.App, image *modal.Image, params *modal.SandboxCreateParams) (sandbox, bool, error) {
	sb, err := p.api.Create(ctx, app, image, params)
	var exists modal.AlreadyExistsError
	if !errors.As(err, &exists) {
		return sb, false, err
	}
	sb, err = p.api.Find(ctx, params.Name)
	return sb, true, err
}

func (p *provider) finishHost(ctx context.Context, sb sandbox, reused bool, request ensureRequest, phases *provisioning) (hostHandle, bool, error) {
	defer sb.Detach()
	var handle hostHandle
	var err error
	if reused {
		var retry bool
		handle, retry, err = readExisting(ctx, sb, request.CanonicalRegion)
		if err != nil || retry {
			return handle, retry, err
		}
		phases.RouteReadAtMS = p.elapsed()
	} else {
		handle, err = p.activate(ctx, sb, request, phases)
		if err != nil {
			return handle, false, err
		}
	}
	phases.HostReadyObservedAtMS = p.elapsed()
	phases.CompletedAtMS = p.elapsed()
	phases.ResourceID, phases.Reused = sb.ID(), reused
	handle.Provisioning = phases
	return handle, false, nil
}

func (p *provider) activate(ctx context.Context, sb sandbox, request ensureRequest, phases *provisioning) (hostHandle, error) {
	route, err := sb.Route(ctx)
	if err != nil {
		return hostHandle{}, err
	}
	if route == "" {
		return hostHandle{}, fmt.Errorf("Modal did not create the durable-object HTTP/2 tunnel")
	}
	if err := sb.WriteFile(ctx, routeFile, route); err != nil {
		return hostHandle{}, err
	}
	phases.RouteReadAtMS = p.elapsed()
	if err := sb.Ready(ctx); err != nil {
		return hostHandle{}, readinessFailure(sb, err)
	}
	return hostHandle{HostID: request.HostID, Route: route, CanonicalRegion: request.CanonicalRegion}, nil
}

func readExisting(ctx context.Context, sb sandbox, region string) (hostHandle, bool, error) {
	exitCode, err := sb.Poll(ctx)
	if err != nil {
		return hostHandle{}, false, err
	}
	if exitCode != nil {
		return hostHandle{}, true, nil
	}
	document, err := sb.Metadata(ctx)
	var handle hostHandle
	if err == nil {
		err = json.Unmarshal(document, &handle)
	}
	if err == nil && handle.CanonicalRegion == region && handle.HostID != "" && handle.Route != "" {
		return handle, false, nil
	}
	if ctx.Err() != nil {
		return hostHandle{}, false, ctx.Err()
	}
	return hostHandle{}, true, sb.Terminate(ctx)
}

func (p *provider) terminateNamed(ctx context.Context, name string) (string, error) {
	sb, err := p.api.Find(ctx, name)
	var missing modal.NotFoundError
	if errors.As(err, &missing) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer sb.Detach()
	if err := sb.Terminate(ctx); err != nil {
		return "", err
	}
	return sb.ID(), nil
}

func hostParams(request ensureRequest) (*modal.SandboxCreateParams, error) {
	region, err := modalRegion(request.CanonicalRegion)
	if err != nil {
		return nil, err
	}
	probe, err := modal.NewExecProbe([]string{"sh", "-c", "test -f " + readyFile}, &modal.ExecProbeParams{Interval: 50 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	bootstrap := `"$1" 2>"$2"; status=$?; if ! test -f "$3"; then sleep 60; fi; exit "$status"`
	return &modal.SandboxCreateParams{
		Name:    resourceName(request.CodeRevision, request.CanonicalRegion),
		Timeout: 24 * time.Hour,
		Command: []string{"sh", "-c", bootstrap, "durable-object-host-bootstrap", "/usr/local/bin/little-actors", stderrFile, readyFile},
		Workdir: request.WorkingDirectory, Env: hostEnvironment(request), H2Ports: []int{7101},
		ReadinessProbe: probe, Regions: []string{region}, Cloud: modalCloud(request.CanonicalRegion),
	}, nil
}

func readinessFailure(sb sandbox, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	detail := strings.TrimSpace(sb.FailureDetail(ctx))
	_ = sb.Terminate(ctx)
	if detail == "" {
		detail = cause.Error()
	}
	return fmt.Errorf("durable-object host did not become ready: %s", detail)
}

func terminateForCleanup(sb sandbox) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = sb.Terminate(ctx)
}

func (p *provider) elapsed() int64 { return elapsed(p.started, p.now()) }
