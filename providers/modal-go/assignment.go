package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type spareAssigner interface {
	Assign(context.Context, spareHandle, map[string]string) (hostHandle, error)
}

type httpSpareAssigner struct{ client *http.Client }

func (a httpSpareAssigner) Assign(ctx context.Context, spare spareHandle, environment map[string]string) (hostHandle, error) {
	if spare.ControlRoute == "" || spare.ControlToken == "" {
		return hostHandle{}, fmt.Errorf("spare assignment endpoint missing")
	}
	endpoint, err := assignmentEndpoint(spare.ControlRoute)
	if err != nil {
		return hostHandle{}, err
	}
	body, err := json.Marshal(environment)
	if err != nil {
		return hostHandle{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return hostHandle{}, err
	}
	request.Header.Set("Authorization", "Bearer "+spare.ControlToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return hostHandle{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return hostHandle{}, fmt.Errorf("spare assignment returned HTTP %d", response.StatusCode)
	}
	document, err := io.ReadAll(io.LimitReader(response.Body, maximumCommandBytes+1))
	if err != nil {
		return hostHandle{}, err
	}
	if len(document) > maximumCommandBytes {
		return hostHandle{}, fmt.Errorf("host readiness is too large")
	}
	var handle hostHandle
	err = json.Unmarshal(document, &handle)
	return handle, err
}

func assignmentEndpoint(route string) (string, error) {
	endpoint, err := url.Parse(route)
	if err != nil {
		return "", fmt.Errorf("invalid spare assignment endpoint: %w", err)
	}
	if endpoint.Scheme != "https" || !strings.HasSuffix(endpoint.Host, ".modal.host") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return "", fmt.Errorf("spare assignment requires a Modal HTTPS tunnel")
	}
	endpoint.Path = "/assign"
	return endpoint.String(), nil
}
