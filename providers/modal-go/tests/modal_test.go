package main

import (
	"context"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	modal "github.com/modal-labs/modal-client/go"
	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestSDKInitializesWithTheRustProvidersSanitizedEnvironment(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("MODAL_CONFIG_PATH", "")
	t.Setenv("MODAL_TOKEN_ID", "test-token")
	t.Setenv("MODAL_TOKEN_SECRET", "test-secret")
	api, closeClient, err := newModalAPI()
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()
	if api == nil {
		t.Fatal("no SDK client")
	}
}

func TestResolveCachesAppAndEachImage(t *testing.T) {
	api, calls := testSDKAPI(t, 0)
	for _, id := range []string{"im-one", "im-two", "im-one", "im-two"} {
		app, image, err := api.Resolve(context.Background(), id)
		if err != nil || app.AppID != "ap-test" || image.ImageID != id {
			t.Fatalf("Resolve(%s) = %v, %v, %v", id, app, image, err)
		}
	}
	if calls.apps.Load() != 1 || calls.images.Load() != 2 {
		t.Fatalf("app/image calls = %d/%d, want 1/2", calls.apps.Load(), calls.images.Load())
	}
}

func TestResolveRetriesFailuresAndCoalescesConcurrentLookups(t *testing.T) {
	api, calls := testSDKAPI(t, time.Millisecond)
	calls.fail.Store(true)
	if _, _, err := api.Resolve(context.Background(), "im-one"); err == nil {
		t.Fatal("lookup failure ignored")
	}
	calls.fail.Store(false)
	group, ctx := errgroup.WithContext(context.Background())
	for range 20 {
		group.Go(func() error { _, _, err := api.Resolve(ctx, "im-one"); return err })
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	apps, images := calls.apps.Load(), calls.images.Load()
	if apps > 2 || images > 2 {
		t.Fatalf("app/image calls = %d/%d, want at most 2/2 including failure", apps, images)
	}
}

func TestRoutesUsesOneTunnelLookupAndRequiresBothPorts(t *testing.T) {
	for _, missing := range []int{0, 7101, 7102} {
		api, calls := testSDKAPI(t, 0)
		for _, port := range []uint32{7101, 7102} {
			if int(port) != missing {
				calls.tunnels = append(calls.tunnels, pb.TunnelData_builder{ContainerPort: port, Host: "tunnel.test", Port: port}.Build())
			}
		}
		sb, err := api.Create(t.Context(), &modal.App{AppID: "ap-test"}, &modal.Image{ImageID: "im-test"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer sb.Detach()
		route, control, err := sb.Routes(t.Context())
		if (err != nil) != (missing != 0) {
			t.Fatalf("missing %d: %v", missing, err)
		}
		if missing == 0 && (route != "https://tunnel.test:7101" || control != "https://tunnel.test:7102") {
			t.Fatalf("routes = %q, %q", route, control)
		}
		if calls.tunnelCalls.Load() != 1 {
			t.Fatalf("tunnel lookups = %d", calls.tunnelCalls.Load())
		}
	}
}

func TestModalMountBeforeReadiness(t *testing.T) {
	if os.Getenv("DURABLE_ACTORS_TEST_MODAL") != "1" {
		t.Skip("set DURABLE_ACTORS_TEST_MODAL=1 to create a short-lived Modal sandbox")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	client, err := modal.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	api := newSDKAPI(client)
	app, err := api.resolveApp(ctx)
	if err != nil {
		t.Fatal(err)
	}
	image, err := client.Images.FromRegistry("alpine:3.21", nil).Build(ctx, app, nil)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := modal.NewExecProbe([]string{"test", "-f", "/customer/etc/alpine-release"}, &modal.ExecProbeParams{Interval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	sb, err := api.Create(ctx, app, image, &modal.SandboxCreateParams{Command: []string{"sleep", "180"}, Timeout: 3 * time.Minute, ReadinessProbe: probe, H2Ports: []int{7101, 7102}, Cloud: "gcp", Regions: []string{"us-east"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Detach()
	defer terminateForCleanup(sb)
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return sb.Mount(ctx, image.ImageID) })
	group.Go(func() error { return readySpare(ctx, sb, &spareHandle{}) })
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

type lookupServer struct {
	pb.UnimplementedModalClientServer
	apps, images atomic.Int64
	tunnelCalls  atomic.Int64
	tunnels      []*pb.TunnelData
	fail         atomic.Bool
	delay        time.Duration
}

func (s *lookupServer) AppGetOrCreate(ctx context.Context, _ *pb.AppGetOrCreateRequest) (*pb.AppGetOrCreateResponse, error) {
	s.apps.Add(1)
	if err := s.lookup(ctx); err != nil {
		return nil, err
	}
	return pb.AppGetOrCreateResponse_builder{AppId: "ap-test"}.Build(), nil
}
func (s *lookupServer) ImageFromId(ctx context.Context, request *pb.ImageFromIdRequest) (*pb.ImageFromIdResponse, error) {
	s.images.Add(1)
	if err := s.lookup(ctx); err != nil {
		return nil, err
	}
	return pb.ImageFromIdResponse_builder{ImageId: request.GetImageId()}.Build(), nil
}
func (s *lookupServer) SandboxCreateV2(context.Context, *pb.SandboxCreateV2Request) (*pb.SandboxCreateV2Response, error) {
	return pb.SandboxCreateV2Response_builder{SandboxId: "sb-01ARZ3NDEKTSV4RRFFQ69G5FAV", TaskId: "ta-test"}.Build(), nil
}
func (s *lookupServer) SandboxGetTunnelsV2(context.Context, *pb.SandboxGetTunnelsRequest) (*pb.SandboxGetTunnelsResponse, error) {
	s.tunnelCalls.Add(1)
	return pb.SandboxGetTunnelsResponse_builder{Tunnels: s.tunnels}.Build(), nil
}

func (s *lookupServer) lookup(ctx context.Context) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.fail.Load() {
		return status.Error(codes.InvalidArgument, "lookup failed")
	}
	return nil
}

func testSDKAPI(t testing.TB, delay time.Duration) (*sdkAPI, *lookupServer) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	calls := &lookupServer{delay: delay}
	pb.RegisterModalClientServer(server, calls)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///modal-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	client, err := modal.NewClientWithOptions(&modal.ClientParams{TokenID: "test", TokenSecret: "test", ControlPlaneClient: pb.NewModalClientClient(conn), ControlPlaneConn: conn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return newSDKAPI(client), calls
}
