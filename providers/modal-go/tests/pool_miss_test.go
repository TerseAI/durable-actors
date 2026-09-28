package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	modal "github.com/modal-labs/modal-client/go"
)

func TestPoolMissOverlapsMountReadinessAndRoutes(t *testing.T) {
	started := make(chan string, 3)
	release := make(chan struct{})
	assigned := make(chan struct{})
	sb := &poolMissSandbox{wait: func(ctx context.Context, phase string) error {
		started <- phase
		wait := release
		if phase == "mount" {
			wait = assigned
		}
		select {
		case <-wait:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	p := newTestProvider(&fakeAPI{created: sb})
	p.assigner = gatedPoolMissAssigner{ready: release, assigned: assigned}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := p.ensureHost(ctx, testRequest()); done <- err }()
	seen := map[string]bool{}
	for len(seen) < 3 {
		select {
		case phase := <-started:
			seen[phase] = true
		case <-ctx.Done():
			<-done
			t.Fatalf("startup phases did not overlap: %v", seen)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if sb.mounts != 1 || sb.mounted != testRequest().CodeSnapshot {
		t.Fatalf("mounted %q %d times", sb.mounted, sb.mounts)
	}
}

func TestPoolMissFailureCancelsStartupBeforeCleanup(t *testing.T) {
	for _, failure := range []string{"mount", "ready", "routes"} {
		t.Run(failure, func(t *testing.T) {
			want := errors.New(failure + " failed")
			started := make(chan struct{}, 3)
			var sb *poolMissSandbox
			sb = &poolMissSandbox{wait: func(ctx context.Context, phase string) error {
				defer sb.recordCall(phase + " stopped")
				started <- struct{}{}
				if phase == failure {
					for range 3 {
						select {
						case <-started:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return want
				}
				<-ctx.Done()
				return ctx.Err()
			}}
			p := newTestProvider(&fakeAPI{created: sb})
			p.assigner = poolMissAssigner{}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := p.ensureHost(ctx, testRequest()); !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
			if len(sb.calls) != 5 || !reflect.DeepEqual(sb.calls[3:], []string{"terminate", "detach"}) {
				t.Fatalf("cleanup = %v", sb.calls)
			}
		})
	}
}

type poolMissSandbox struct {
	fakeSandbox
	wait   func(context.Context, string) error
	mounts int
}

func (s *poolMissSandbox) Ready(ctx context.Context) error { return s.wait(ctx, "ready") }
func (s *poolMissSandbox) Routes(ctx context.Context) (string, string, error) {
	return "https://host.test", "https://control.test", s.wait(ctx, "routes")
}
func (s *poolMissSandbox) Mount(ctx context.Context, snapshot string) error {
	s.mounts++
	s.mounted = snapshot
	return s.wait(ctx, "mount")
}

type gatedPoolMissAssigner struct {
	poolMissAssigner
	ready, assigned chan struct{}
}

func (a gatedPoolMissAssigner) Assign(ctx context.Context, spare spareHandle, env map[string]string) (hostHandle, error) {
	select {
	case <-a.ready:
	default:
		return hostHandle{}, errors.New("assigned before readiness and routes")
	}
	close(a.assigned)
	return a.poolMissAssigner.Assign(ctx, spare, env)
}

type poolMissAssigner struct{}

func (poolMissAssigner) Assign(_ context.Context, spare spareHandle, env map[string]string) (hostHandle, error) {
	return hostHandle{
		HostID: env["DURABLE_ACTORS_HOST_ID"], SessionID: env["DURABLE_ACTORS_SESSION_ID"],
		Route: spare.Route, CanonicalRegion: spare.CanonicalRegion, OwnerEpoch: 42,
		Lease: &activationLease{ID: env["DURABLE_ACTORS_HOST_ID"], SessionID: env["DURABLE_ACTORS_SESSION_ID"], Route: spare.Route, ExpiresAtMS: uint64(time.Now().Add(time.Minute).UnixMilli())},
	}, nil
}
func (poolMissAssigner) Warm(context.Context, spareHandle) {}

// Model each remote operation as 10 ms; use the same benchmark on both revisions.
func BenchmarkPoolMiss(b *testing.B) {
	for _, warm := range []bool{false, true} {
		name := "cold-worker"
		if warm {
			name = "warm-worker"
		}
		b.Run(name, func(b *testing.B) {
			sdk, calls := testSDKAPI(b, 10*time.Millisecond)
			api := &poolMissAPI{fakeAPI: fakeAPI{created: &poolMissSandbox{wait: func(ctx context.Context, _ string) error { return poolMissDelay(ctx) }}}, sdk: sdk}
			p := newTestProvider(api)
			p.assigner = timedPoolMissAssigner{}
			if warm {
				if _, _, err := sdk.Resolve(context.Background(), "im-test"); err != nil {
					b.Fatal(err)
				}
			}
			calls.apps.Store(0)
			calls.images.Store(0)
			b.ResetTimer()
			for range b.N {
				if !warm {
					api.sdk = newSDKAPI(sdk.client)
				}
				if _, err := p.ensureHost(context.Background(), testRequest()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(calls.apps.Load())/float64(b.N), "app-lookups/op")
			b.ReportMetric(float64(calls.images.Load())/float64(b.N), "image-lookups/op")
		})
	}
}

type poolMissAPI struct {
	fakeAPI
	sdk *sdkAPI
}

func (a *poolMissAPI) Resolve(ctx context.Context, id string) (*modal.App, *modal.Image, error) {
	return a.sdk.Resolve(ctx, id)
}
func (a *poolMissAPI) Create(ctx context.Context, app *modal.App, image *modal.Image, params *modal.SandboxCreateParams) (sandbox, error) {
	if err := poolMissDelay(ctx); err != nil {
		return nil, err
	}
	return a.fakeAPI.Create(ctx, app, image, params)
}

type timedPoolMissAssigner struct{ poolMissAssigner }

func (a timedPoolMissAssigner) Assign(ctx context.Context, spare spareHandle, env map[string]string) (hostHandle, error) {
	if err := poolMissDelay(ctx); err != nil {
		return hostHandle{}, err
	}
	return a.poolMissAssigner.Assign(ctx, spare, env)
}
func poolMissDelay(ctx context.Context) error {
	select {
	case <-time.After(10 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
