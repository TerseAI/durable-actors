package main

import (
	"context"
	"encoding/json"
	"errors"
	modal "github.com/modal-labs/modal-client/go"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestNamedSecretsUseANewGenericSandbox(t *testing.T) {
	sb := &fakeSandbox{}
	api := &fakeAPI{created: sb}
	request := testRequest()
	request.SecretRefs = []string{"project-secrets"}
	if _, err := newTestProvider(api).ensureHost(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(api.params.Secrets) != 1 || api.params.Secrets[0].Name != "project-secrets" {
		t.Fatal("missing secret")
	}
	if api.params.Env["DURABLE_OBJECT_PROCESS_ROLE"] != "spare" {
		t.Fatal("not generic execution")
	}
}

func TestAssignmentRequiresActorAndPublishedCodeBeforeCallingModal(t *testing.T) {
	for _, field := range []string{"actor", "code", "entrypoint", "session"} {
		request := testRequest()
		switch field {
		case "session":
			request.SessionID = ""
		case "actor":
			request.Actor = nil
		case "code":
			request.CodeSnapshot = ""
		case "entrypoint":
			request.ActorEntrypoint = "../escape.mjs"
		}
		api := &fakeAPI{}
		if _, err := newTestProvider(api).ensureHost(context.Background(), request); err == nil {
			t.Fatal(field)
		}
		if api.creates != 0 || api.resolves != 0 {
			t.Fatal("invalid request touched Modal")
		}
	}
}

func TestMountAndAssignmentOverlapAndFailureTerminatesTheClaimedSpare(t *testing.T) {
	sb := &fakeSandbox{mountErr: errors.New("mount failed"), mountStarted: make(chan struct{}), assignmentStarted: make(chan struct{})}
	api := &fakeAPI{found: sb}
	request := testRequest()
	request.Spare = &spareHandle{ResourceID: "sb-test", Route: "https://host.test", CanonicalRegion: request.CanonicalRegion}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := newTestProvider(api).ensureHost(ctx, request); err == nil {
		t.Fatal("mount failure was ignored")
	}
	if !reflect.DeepEqual(sb.calls, []string{"terminate", "detach"}) {
		t.Fatal(sb.calls)
	}
}

func newTestProvider(api modalAPI) *provider {
	return &provider{api: api, assigner: fakeAssigner{api}, now: time.Now, started: time.Now()}
}
func testRequest() ensureRequest {
	return ensureRequest{SessionID: "00000000-0000-4000-8000-000000000001", Actor: json.RawMessage(`{"actor_type":"Counter","actor_id":"one"}`), CodeSnapshot: "im-code", WorkingDirectory: "/customer", ActorEntrypoint: "actors.mjs", Resources: resourceLimits{CPUMillis: 1000, MemoryMiB: 1024}, CodeRevision: "r1", CanonicalRegion: "north-america-east", HostID: "host.v3.r1.new", HostToken: "test-token", ImageRef: "im-test", ActorIdleTimeoutSeconds: 60, HostIdleTimeoutMS: 300000}
}

type fakeAPI struct {
	created, found                         sandbox
	createErr, findErr                     error
	creates, finds, resolves, succeedAfter int
	params                                 *modal.SandboxCreateParams
	name                                   string
}

func (a *fakeAPI) Resolve(_ context.Context, id string) (*modal.App, *modal.Image, error) {
	a.resolves++
	return &modal.App{}, &modal.Image{ImageID: id}, nil
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
	controlRoute      string
	mounted           string
	assignment        map[string]string
	mountErr          error
	mountStarted      chan struct{}
	assignmentStarted chan struct{}
	calls             []string
	metadata          string
	buildErr          error
	snapshotErr       error
	contract          json.RawMessage
}

func TestSocketCredentialsRequireTheResolvedHostSession(t *testing.T) {
	r := socketRequest{ResourceID: "sb-test", CanonicalRegion: "north-america-east", HostID: "host.v3.revision.original", SessionID: "session"}
	for _, session := range []string{"session", "replacement"} {
		sb := &fakeSandbox{metadata: `{"hostId":"host.v3.revision.original","sessionId":"` + session + `","canonicalRegion":"north-america-east"}`}
		api := &fakeAPI{found: sb}
		credentials, err := newTestProvider(api).socketCredentials(context.Background(), r)
		if session == "replacement" {
			if err == nil {
				t.Fatal("credentials issued for a replaced host session")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if credentials.URL != "https://connect.test" || credentials.Token != "connect-token" || api.creates != 0 {
			t.Fatalf("unexpected credentials: %+v", credentials)
		}
		if api.name != r.ResourceID {
			t.Fatal(api.name)
		}
	}
}

func (s *fakeSandbox) Connect(context.Context) (socketCredentials, error) {
	s.calls = append(s.calls, "connect")
	return socketCredentials{URL: "https://connect.test", Token: "connect-token"}, nil
}

func (s *fakeSandbox) ID() string { return "sb-test" }
func (s *fakeSandbox) Route(context.Context) (string, error) {
	s.calls = append(s.calls, "route")
	return "https://host.test", nil
}
func (s *fakeSandbox) Ready(context.Context) error { s.calls = append(s.calls, "ready"); return nil }
func (s *fakeSandbox) Metadata(context.Context) ([]byte, error) {
	s.calls = append(s.calls, "metadata")
	return json.RawMessage(s.metadata), nil
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

func TestHostIdentityUsesOnlyRevisionAndSession(t *testing.T) {
	r := testRequest()
	r.HostID = "host.v3.r1.session"
	if err := validateEnsure(r); err != nil {
		t.Fatal(err)
	}
}

func (a *fakeAPI) ByID(ctx context.Context, id string) (sandbox, error) { return a.Find(ctx, id) }
func (s *fakeSandbox) Mount(ctx context.Context, image *modal.Image) error {
	s.mounted = image.ImageID
	if s.mountStarted != nil {
		close(s.mountStarted)
		select {
		case <-s.assignmentStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.mountErr
}
func (s *fakeSandbox) assign(ctx context.Context, environment map[string]string) error {
	s.assignment = environment
	if s.assignmentStarted != nil {
		close(s.assignmentStarted)
		select {
		case <-s.mountStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (s *fakeSandbox) Snapshot(context.Context) (string, error) {
	s.calls = append(s.calls, "snapshot")
	return "im-code", s.snapshotErr
}

func (s *fakeSandbox) BuildCode(_ context.Context, directory, entrypoint string) (json.RawMessage, error) {
	s.calls = append(s.calls, "build:"+directory+":"+entrypoint)
	if s.contract != nil {
		return s.contract, s.buildErr
	}
	return json.RawMessage(`{"version":1,"actors":[]}`), s.buildErr
}

func (s *fakeSandbox) ControlRoute(context.Context) (string, error) {
	if s.controlRoute != "" {
		return s.controlRoute, nil
	}
	return "https://control.test", nil
}

type fakeAssigner struct{ api modalAPI }

func (a fakeAssigner) Assign(ctx context.Context, spare spareHandle, environment map[string]string) (hostHandle, error) {
	api := a.api
	if policy, ok := api.(*networkPolicyAPI); ok {
		api = policy.modalAPI
	}
	fixture := api.(*fakeAPI)
	sb := fixture.found
	if sb == nil {
		sb = fixture.created
	}
	fake := sb.(*fakeSandbox)
	if err := fake.assign(ctx, environment); err != nil {
		return hostHandle{}, err
	}
	if fake.metadata != "" {
		var handle hostHandle
		err := json.Unmarshal([]byte(fake.metadata), &handle)
		return handle, err
	}
	return hostHandle{Lease: &activationLease{ID: environment["DURABLE_OBJECT_HOST_ID"], SessionID: environment["DURABLE_OBJECT_SESSION_ID"], Route: environment["DURABLE_OBJECT_HOST_ROUTE"], ExpiresAtMS: uint64(time.Now().Add(time.Minute).UnixMilli())}, HostID: environment["DURABLE_OBJECT_HOST_ID"], SessionID: environment["DURABLE_OBJECT_SESSION_ID"], Route: environment["DURABLE_OBJECT_HOST_ROUTE"], CanonicalRegion: environment["DURABLE_OBJECT_REGION"], OwnerEpoch: 42}, nil
}

func assignmentServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var environment map[string]string
		if err := json.NewDecoder(r.Body).Decode(&environment); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		json.NewEncoder(w).Encode(hostHandle{Lease: &activationLease{ID: environment["DURABLE_OBJECT_HOST_ID"], SessionID: environment["DURABLE_OBJECT_SESSION_ID"], Route: environment["DURABLE_OBJECT_HOST_ROUTE"], ExpiresAtMS: uint64(time.Now().Add(time.Minute).UnixMilli())}, HostID: environment["DURABLE_OBJECT_HOST_ID"], SessionID: environment["DURABLE_OBJECT_SESSION_ID"], Route: environment["DURABLE_OBJECT_HOST_ROUTE"], CanonicalRegion: environment["DURABLE_OBJECT_REGION"], OwnerEpoch: 42})
	}))
	t.Cleanup(server.Close)
	return server
}
