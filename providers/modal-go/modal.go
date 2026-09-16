package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	modal "github.com/modal-labs/modal-client/go"
	"golang.org/x/sync/errgroup"
)

type sdkAPI struct{ client *modal.Client }

func newModalAPI() (modalAPI, func(), error) {
	mutable := false
	if value := os.Getenv("DURABLE_OBJECT_MODAL_MUTABLE_NETWORK"); value != "" {
		var err error
		mutable, err = strconv.ParseBool(value)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid mutable network configuration: %w", err)
		}
	}
	// The Rust parent supplies credentials in a sanitized environment without HOME.
	if err := os.Setenv("MODAL_CONFIG_PATH", os.DevNull); err != nil {
		return nil, nil, err
	}
	client, err := modal.NewClient()
	if err != nil {
		return nil, nil, err
	}
	return &networkPolicyAPI{modalAPI: &sdkAPI{client: client}, mutable: mutable}, client.Close, nil
}

func (a *sdkAPI) Resolve(ctx context.Context, imageID string) (*modal.App, *modal.Image, error) {
	group, ctx := errgroup.WithContext(ctx)
	var app *modal.App
	var image *modal.Image
	group.Go(func() error {
		var err error
		app, err = a.client.Apps.FromName(ctx, appName, &modal.AppFromNameParams{CreateIfMissing: true})
		return err
	})
	group.Go(func() error { var err error; image, err = a.client.Images.FromID(ctx, imageID, nil); return err })
	err := group.Wait()
	return app, image, err
}

func (a *sdkAPI) Create(ctx context.Context, app *modal.App, image *modal.Image, params *modal.SandboxCreateParams) (sandbox, error) {
	sb, err := a.client.Sandboxes.ExperimentalCreate(ctx, app, image, params)
	if err != nil {
		return nil, err
	}
	return &sdkSandbox{sb}, nil
}

func (a *sdkAPI) Find(ctx context.Context, name string) (sandbox, error) {
	sb, err := a.client.Sandboxes.ExperimentalFromName(ctx, appName, name, nil)
	if err != nil {
		return nil, err
	}
	return &sdkSandbox{sb}, nil
}

func (a *sdkAPI) Secret(ctx context.Context, name string) (*modal.Secret, error) {
	return a.client.Secrets.FromName(ctx, name, nil)
}

type sdkSandbox struct{ sb *modal.Sandbox }

func (s *sdkSandbox) ID() string                             { return s.sb.SandboxID }
func (s *sdkSandbox) Detach()                                { _ = s.sb.Detach() }
func (s *sdkSandbox) Poll(ctx context.Context) (*int, error) { return s.sb.Poll(ctx, nil) }
func (s *sdkSandbox) Wait(ctx context.Context) (int, error)  { return s.sb.Wait(ctx, nil) }
func (s *sdkSandbox) Terminate(ctx context.Context) error {
	_, err := s.sb.Terminate(ctx, nil)
	return err
}
func (s *sdkSandbox) Ready(ctx context.Context) error {
	return s.sb.WaitUntilReady(ctx, time.Minute, nil)
}
func (s *sdkSandbox) WriteFile(ctx context.Context, path, data string) error {
	return s.sb.Filesystem.WriteText(ctx, data, path, nil)
}

func (s *sdkSandbox) Route(ctx context.Context) (string, error) {
	tunnels, err := s.sb.Tunnels(ctx, 50*time.Second, nil)
	if err != nil {
		return "", err
	}
	if tunnel := tunnels[7101]; tunnel != nil {
		return tunnel.URL(), nil
	}
	return "", fmt.Errorf("Modal did not create the durable-object HTTP/2 tunnel")
}

func (s *sdkSandbox) Metadata(ctx context.Context) ([]byte, error) {
	command := "for i in $(seq 1 1200); do test -f " + readyFile + " && test -s " + metadataFile + " && exec cat " + metadataFile + "; sleep 0.05; done; exit 1"
	process, err := s.sb.Exec(ctx, []string{"sh", "-c", command}, &modal.SandboxExecParams{Stdout: modal.Pipe, Stderr: modal.Ignore, Timeout: time.Minute})
	if err != nil {
		return nil, err
	}
	defer process.Stdout.Close()
	document, err := io.ReadAll(io.LimitReader(process.Stdout, maximumCommandBytes+1))
	if err != nil {
		return nil, err
	}
	if len(document) > maximumCommandBytes {
		return nil, fmt.Errorf("host metadata is too large")
	}
	exitCode, err := process.Wait(ctx, nil)
	if err != nil {
		return nil, err
	}
	if exitCode != 0 {
		return nil, fmt.Errorf("existing Modal host has no ready metadata")
	}
	return document, nil
}

func (s *sdkSandbox) FailureDetail(ctx context.Context) string {
	detail, _ := s.sb.Filesystem.ReadText(ctx, stderrFile, nil)
	return detail
}
