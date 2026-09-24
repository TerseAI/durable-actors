package main

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

type spareHandles struct {
	mu       sync.Mutex
	entries  *simplelru.LRU[string, retainedHandle]
	now      func() time.Time
	ttl      time.Duration
	capacity int
	closed   bool
}

type retainedHandle struct {
	sandbox sandbox
	expires time.Time
}

func newSpareHandles(capacity int, ttl time.Duration, now func() time.Time) *spareHandles {
	entries, err := simplelru.NewLRU[string, retainedHandle](capacity, nil)
	if err != nil {
		panic(err)
	}
	return &spareHandles{entries: entries, capacity: capacity, ttl: ttl, now: now}
}

func (h *spareHandles) keep(sb sandbox) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		sb.Detach()
		return
	}
	h.expire()
	if old, ok := h.entries.Peek(sb.ID()); ok {
		h.entries.Remove(sb.ID())
		old.sandbox.Detach()
	}
	if h.entries.Len() == h.capacity {
		_, old, _ := h.entries.RemoveOldest()
		old.sandbox.Detach()
	}
	h.entries.Add(sb.ID(), retainedHandle{sandbox: sb, expires: h.now().Add(h.ttl)})
}

func (h *spareHandles) take(id string) sandbox {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.entries.Peek(id)
	if !ok {
		return nil
	}
	h.entries.Remove(id)
	if !h.now().Before(entry.expires) {
		entry.sandbox.Detach()
		return nil
	}
	return entry.sandbox
}

func (h *spareHandles) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for h.entries.Len() > 0 {
		_, entry, _ := h.entries.RemoveOldest()
		entry.sandbox.Detach()
	}
}

func (h *spareHandles) maintain(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.mu.Lock()
			h.expire()
			h.mu.Unlock()
		}
	}
}

func (h *spareHandles) expire() {
	for {
		id, entry, ok := h.entries.GetOldest()
		if !ok || h.now().Before(entry.expires) {
			return
		}
		h.entries.Remove(id)
		entry.sandbox.Detach()
	}
}
