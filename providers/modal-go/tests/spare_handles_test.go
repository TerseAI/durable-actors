package main

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

type identifiedSandbox struct {
	*fakeSandbox
	id string
}

func (s *identifiedSandbox) ID() string { return s.id }

func TestSpareHandlesEvictAndExpireWithoutTerminatingSandboxes(t *testing.T) {
	now := time.Unix(1, 0)
	handles := newSpareHandles(2, time.Minute, func() time.Time { return now })
	defer handles.close()
	first := &identifiedSandbox{&fakeSandbox{}, "one"}
	second := &identifiedSandbox{&fakeSandbox{}, "two"}
	third := &identifiedSandbox{&fakeSandbox{}, "three"}
	handles.keep(first)
	handles.keep(second)
	handles.keep(third)
	if !slices.Equal(first.calls, []string{"detach"}) {
		t.Fatal(first.calls)
	}
	if handles.take("one") != nil {
		t.Fatal("evicted handle returned")
	}
	if handles.take("two") != second || len(second.calls) != 0 {
		t.Fatal("claim detached the handle")
	}
	now = now.Add(time.Minute)
	if handles.take("three") != nil || !slices.Equal(third.calls, []string{"detach"}) {
		t.Fatal("expired handle returned")
	}
}

func TestSpareHandleHasOneOwnerAndShutdownClosesUnclaimedHandles(t *testing.T) {
	handles := newSpareHandles(2, time.Hour, time.Now)
	claimed := &identifiedSandbox{&fakeSandbox{}, "claimed"}
	idle := &identifiedSandbox{&fakeSandbox{}, "idle"}
	handles.keep(claimed)
	handles.keep(idle)
	owners := make(chan sandbox, 8)
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() { defer group.Done(); owners <- handles.take("claimed") }()
	}
	group.Wait()
	close(owners)
	count := 0
	for owner := range owners {
		if owner != nil {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("owners: %d", count)
	}
	handles.close()
	if len(claimed.calls) != 0 || !slices.Equal(idle.calls, []string{"detach"}) {
		t.Fatal("shutdown changed the claimed handle")
	}
	late := &fakeSandbox{}
	handles.keep(late)
	if !slices.Equal(late.calls, []string{"detach"}) {
		t.Fatal("shutdown retained a late handle")
	}
}

func TestRetirementConsumesTheCachedHandle(t *testing.T) {
	sb := &fakeSandbox{}
	api := &fakeAPI{created: sb, found: sb}
	p := newTestProvider(api)
	p.handles.keep(sb)
	if err := p.retireSpare(context.Background(), spareHandle{ResourceID: sb.ID()}); err != nil {
		t.Fatal(err)
	}
	if api.finds != 0 || !slices.Equal(sb.calls, []string{"terminate", "detach"}) {
		t.Fatal(api.finds, sb.calls)
	}
	if p.handles.take(sb.ID()) != nil {
		t.Fatal("retired handle retained")
	}
}
