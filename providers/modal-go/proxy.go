package main

import (
	"context"
	"encoding/json"
	"fmt"
)

type proxyRequest struct {
	spareRequest
	Config json.RawMessage `json:"config"`
}

func (p *provider) ensureProxy(ctx context.Context, request proxyRequest) (spareHandle, error) {
	var config struct {
		Session string `json:"session"`
		Region  string `json:"region"`
	}
	if err := json.Unmarshal(request.Config, &config); err != nil || config.Session == "" || config.Region != request.CanonicalRegion {
		return spareHandle{}, fmt.Errorf("invalid proxy configuration")
	}
	request.Kind = "proxy"
	params, err := spareParams(request.spareRequest)
	if err != nil {
		return spareHandle{}, err
	}
	params.Env["DURABLE_ACTORS_PROXY_CONFIG"] = string(request.Config)
	app, image, err := p.api.Resolve(ctx, request.ImageRef)
	if err != nil {
		return spareHandle{}, err
	}
	sb, err := p.api.Create(ctx, app, image, params)
	if err != nil {
		return spareHandle{}, err
	}
	defer sb.Detach()
	ready := false
	defer func() {
		if !ready {
			terminateForCleanup(sb)
		}
	}()
	if err := sb.Ready(ctx); err != nil {
		return spareHandle{}, err
	}
	route, err := sb.Route(ctx)
	if err != nil || route == "" {
		return spareHandle{}, fmt.Errorf("proxy route unavailable: %v", err)
	}
	ready = true
	return spareHandle{
		Name: request.Name, ResourceID: sb.ID(), Route: route, CanonicalRegion: request.CanonicalRegion,
	}, nil
}
