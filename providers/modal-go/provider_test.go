package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	modal "github.com/modal-labs/modal-client/go"
)

func TestFreshHostPublishesOnlyRouteAndWaitsForReadiness(t *testing.T) {
	sb := &fakeSandbox{}
	api := &fakeAPI{created: sb}
	p := newTestProvider(api)
	r := testRequest()
	handle, err := p.ensureHost(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if handle.HostID != r.HostID || handle.Route != "https://host.test" || handle.Provisioning.Reused {
		t.Fatalf("unexpected handle: %+v", handle)
	}
	if api.finds != 0 || api.creates != 1 {
		t.Fatalf("lookups=%d creates=%d", api.finds, api.creates)
	}
	if !reflect.DeepEqual(sb.calls, []string{"route", "write:" + routeFile, "ready", "detach"}) {
		t.Fatal(sb.calls)
	}
	if api.params.Cloud != "" || !reflect.DeepEqual(api.params.Regions, []string{"us-east"}) || !reflect.DeepEqual(api.params.H2Ports, []int{7101}) {
		t.Fatalf("unexpected placement: %+v", api.params)
	}
	if api.params.CPU != 0 || api.params.Timeout != 24*time.Hour || api.params.ReadinessProbe == nil {
		t.Fatal("changed sandbox defaults")
	}
	if api.params.Env["DURABLE_OBJECT_HOST_METADATA_FILE"] != metadataFile || api.params.Env["DURABLE_OBJECT_HOST_TOKEN"] != r.HostToken {
		t.Fatal(api.params.Env)
	}
}

func TestHostAttachesNamedSecretsAndSocketGateway(t *testing.T) {
	api := &fakeAPI{created: &fakeSandbox{}}
	request := testRequest()
	request.SecretRefs = []string{"project-secrets"}
	request.SocketGatewayURL = "https://sockets.example"
	if _, err := newTestProvider(api).ensureHost(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(api.params.Secrets) != 1 || api.params.Secrets[0].Name != "project-secrets" {
		t.Fatal("secret reference was not attached")
	}
	if api.params.Env["DURABLE_OBJECT_SOCKET_GATEWAY_URL"] != request.SocketGatewayURL {
		t.Fatal("gateway address was lost")
	}
}

func TestExistingHostKeepsItsIdentity(t *testing.T) {
	r := testRequest()
	existing := &fakeSandbox{metadata: `{"hostId":"host.v2.qa:existing","route":"https://existing.test","canonicalRegion":"north-america-east"}`}
	api := &fakeAPI{createErr: modal.AlreadyExistsError{}, found: existing}
	handle, err := newTestProvider(api).ensureHost(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if handle.HostID != "host.v2.qa:existing" || !handle.Provisioning.Reused {
		t.Fatalf("unexpected handle: %+v", handle)
	}
	if !reflect.DeepEqual(existing.calls, []string{"poll", "metadata", "detach"}) {
		t.Fatal(existing.calls)
	}
}

func TestAmbiguousCreateFailureIsNotRetried(t *testing.T) {
	api := &fakeAPI{createErr: errors.New("connection lost")}
	if _, err := newTestProvider(api).ensureHost(context.Background(), testRequest()); err == nil {
		t.Fatal("expected failure")
	}
	if api.creates != 1 || api.finds != 0 {
		t.Fatal("ambiguous create was retried")
	}
}

func TestStatusFailureDoesNotTerminateExistingHost(t *testing.T) {
	sb := &fakeSandbox{pollErr: errors.New("status unavailable")}
	api := &fakeAPI{createErr: modal.AlreadyExistsError{}, found: sb}
	if _, err := newTestProvider(api).ensureHost(context.Background(), testRequest()); err == nil {
		t.Fatal("expected failure")
	}
	if !reflect.DeepEqual(sb.calls, []string{"poll", "detach"}) || api.creates != 1 {
		t.Fatal(sb.calls)
	}
}

func TestKnownFailedHostIsReplacedOnce(t *testing.T) {
	failed := &fakeSandbox{metadata: `{}`}
	created := &fakeSandbox{}
	api := &fakeAPI{created: created, found: failed, createErr: modal.AlreadyExistsError{}, succeedAfter: 1}
	if _, err := newTestProvider(api).ensureHost(context.Background(), testRequest()); err != nil {
		t.Fatal(err)
	}
	if api.creates != 2 || !reflect.DeepEqual(failed.calls, []string{"poll", "metadata", "terminate", "detach"}) {
		t.Fatal(failed.calls)
	}
}

func TestInvalidHostRequestsDoNotTouchModal(t *testing.T) {
	for _, change := range []func(*ensureRequest){func(r *ensureRequest) { r.HostID = "other" }, func(r *ensureRequest) { r.CanonicalRegion = "unknown" }, func(r *ensureRequest) { r.HostIdleTimeoutMS = 0 }, func(r *ensureRequest) { r.ActorIdleTimeoutMS = 86400001 }} {
		r := testRequest()
		change(&r)
		api := &fakeAPI{}
		if _, err := newTestProvider(api).ensureHost(context.Background(), r); err == nil {
			t.Fatal("expected validation error")
		}
		if api.resolves != 0 {
			t.Fatal("invalid request touched Modal")
		}
	}
}

func TestHostsAndWarmupsUseRegionalPlacement(t *testing.T) {
	for _, tc := range []struct {
		canonicalRegion, region, cloud string
	}{
		{"north-america-east", "us-east", ""},
		{"north-america-central", "us-central", "gcp"},
		{"north-america-south", "us-south", "gcp"},
		{"north-america-west", "us-west", "gcp"},
		{"europe-west", "eu-west", "gcp"},
		{"asia-southeast", "ap-southeast", "gcp"},
	} {
		t.Run(tc.canonicalRegion, func(t *testing.T) {
			r := testRequest()
			r.CanonicalRegion = tc.canonicalRegion
			params, err := hostParams(r)
			if err != nil {
				t.Fatal(err)
			}
			if params.Cloud != tc.cloud || !reflect.DeepEqual(params.Regions, []string{tc.region}) {
				t.Fatalf("unexpected host placement: cloud=%q regions=%v", params.Cloud, params.Regions)
			}
			api := &fakeAPI{created: &fakeSandbox{}}
			_, err = newTestProvider(api).warmImage(context.Background(), imageRequest{
				NamespaceID: r.NamespaceID, CodeRevision: r.CodeRevision,
				CanonicalRegion: r.CanonicalRegion, ImageRef: r.ImageRef,
			})
			if err != nil {
				t.Fatal(err)
			}
			if api.params.Cloud != tc.cloud || !reflect.DeepEqual(api.params.Regions, []string{tc.region}) {
				t.Fatalf("unexpected warmup placement: cloud=%q regions=%v", api.params.Cloud, api.params.Regions)
			}
		})
	}
}

func TestWarmupAlwaysTerminatesAndDetaches(t *testing.T) {
	for _, exit := range []int{0, 1} {
		sb := &fakeSandbox{exit: exit}
		api := &fakeAPI{created: sb}
		_, err := newTestProvider(api).warmImage(context.Background(), imageRequest{NamespaceID: "qa", CodeRevision: "r1", CanonicalRegion: "north-america-west", ImageRef: "im-test"})
		if (err != nil) != (exit != 0) {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(sb.calls, []string{"wait", "terminate", "detach"}) {
			t.Fatal(sb.calls)
		}
		if !reflect.DeepEqual(api.params.Command, []string{"true"}) || api.params.Timeout != 2*time.Minute {
			t.Fatal("wrong primer")
		}
	}
}

func TestTerminateUsesExactNamesAndIgnoresMissingHosts(t *testing.T) {
	api := &fakeAPI{findErr: modal.NotFoundError{}}
	result, err := newTestProvider(api).terminateHosts(context.Background(), terminateRequest{NamespaceID: "qa", CodeRevision: "r1", CanonicalRegions: []string{"north-america-east"}})
	if err != nil || len(result.ResourceIDs) != 0 {
		t.Fatal(result, err)
	}
	if api.name != resourceName("qa", "r1", "north-america-east") {
		t.Fatal(api.name)
	}
}

func TestResourceNamesMatchJavaScript(t *testing.T) {
	if got := resourceName("qa", "r1", "north-america-east"); got != "do-host-v2-a4e56f1e61a3a5e94383080204b7bd4c" {
		t.Fatal(got)
	}
}

func newTestProvider(api modalAPI) *provider {
	return &provider{api: api, now: time.Now, started: time.Now()}
}
func testRequest() ensureRequest {
	return ensureRequest{NamespaceID: "qa", CodeRevision: "r1", CanonicalRegion: "north-america-east", HostID: "host.v2.qa:new", HostToken: "test-token", ImageRef: "im-test", ActorIdleTimeoutMS: 60000, HostIdleTimeoutMS: 300000}
}

type fakeAPI struct {
	created, found                         sandbox
	createErr, findErr                     error
	creates, finds, resolves, succeedAfter int
	params                                 *modal.SandboxCreateParams
	name                                   string
}

func (a *fakeAPI) Resolve(context.Context, string) (*modal.App, *modal.Image, error) {
	a.resolves++
	return &modal.App{}, &modal.Image{}, nil
}
func (a *fakeAPI) Secret(_ context.Context, name string) (*modal.Secret, error) {
	return &modal.Secret{Name: name}, nil
}
func (a *fakeAPI) Create(_ context.Context, _ *modal.App, _ *modal.Image, params *modal.SandboxCreateParams) (sandbox, error) {
	a.creates++
	a.params = params
	if a.createErr != nil && (a.succeedAfter == 0 || a.creates <= a.succeedAfter) {
		return nil, a.createErr
	}
	return a.created, nil
}
func (a *fakeAPI) Find(_ context.Context, name string) (sandbox, error) {
	a.finds++
	a.name = name
	return a.found, a.findErr
}

type fakeSandbox struct {
	calls    []string
	metadata string
	pollErr  error
	exit     int
}

func (s *fakeSandbox) ID() string { return "sb-test" }
func (s *fakeSandbox) Route(context.Context) (string, error) {
	s.calls = append(s.calls, "route")
	return "https://host.test", nil
}
func (s *fakeSandbox) WriteFile(_ context.Context, path, _ string) error {
	s.calls = append(s.calls, "write:"+path)
	return nil
}
func (s *fakeSandbox) Ready(context.Context) error { s.calls = append(s.calls, "ready"); return nil }
func (s *fakeSandbox) Metadata(context.Context) ([]byte, error) {
	s.calls = append(s.calls, "metadata")
	return json.RawMessage(s.metadata), nil
}
func (s *fakeSandbox) FailureDetail(context.Context) string { return "" }
func (s *fakeSandbox) Poll(context.Context) (*int, error) {
	s.calls = append(s.calls, "poll")
	return nil, s.pollErr
}
func (s *fakeSandbox) Wait(context.Context) (int, error) {
	s.calls = append(s.calls, "wait")
	return s.exit, nil
}
func (s *fakeSandbox) Terminate(context.Context) error {
	s.calls = append(s.calls, "terminate")
	return nil
}
func (s *fakeSandbox) Detach() { s.calls = append(s.calls, "detach") }

func TestMutableNetworkIsOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		api := &fakeAPI{created: &fakeSandbox{}}
		p := newTestProvider(&networkPolicyAPI{modalAPI: api, mutable: enabled})
		if _, err := p.ensureHost(context.Background(), testRequest()); err != nil {
			t.Fatal(err)
		}
		if !enabled {
			if api.params.OutboundCIDRAllowlist != nil || api.params.OutboundDomainAllowlist != nil {
				t.Fatal("ordinary hosts must retain default open networking")
			}
		} else if api.params.OutboundCIDRAllowlist == nil || api.params.OutboundDomainAllowlist == nil || !reflect.DeepEqual(api.params.OutboundCIDRAllowlist.Entries, []string{"0.0.0.0/0"}) || !reflect.DeepEqual(api.params.OutboundDomainAllowlist.Entries, []string{"*"}) {
			t.Fatal("opted-in hosts must start with an allow-all policy")
		}
	}
}
