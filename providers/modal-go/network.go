package main

import (
	"context"
	modal "github.com/modal-labs/modal-client/go"
)

type networkPolicyAPI struct {
	modalAPI
	mutable bool
}

func (a *networkPolicyAPI) Create(ctx context.Context, app *modal.App, image *modal.Image, params *modal.SandboxCreateParams) (sandbox, error) {
	if a.mutable {
		// Modal requires allowlist mode at creation to support later policy changes.
		params.OutboundCIDRAllowlist = &modal.Allowlist{Entries: []string{"0.0.0.0/0"}}
		params.OutboundDomainAllowlist = &modal.Allowlist{Entries: []string{"*"}}
	}
	return a.modalAPI.Create(ctx, app, image, params)
}
