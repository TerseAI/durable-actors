# Overview backend handoff

Design reference: [little-actors · Overview](https://doop.design/c/x71to_CeX0), frame `xIP9Ls3hDj`.

This UI implementation uses only the existing inventory and request-trace APIs. The observer API executes read-only SQL from the UI for saved history and provides durable live replay cursors. The current surface deliberately does not display the mockup's fabricated deployment, history, identity, or write metrics.

## Available and wired

- `/api/observe/events` proxies `/v1/observe/events`: actor classes, live/dormant/unknown counts, instance IDs, active connection IDs and metadata. Clients without subscriptions retain five-second inventory polling.
- `/api/observe/requests/events` proxies `/v1/observe/requests/events`: method/WebSocket event attempts, outcome, total duration, queue wait, timestamp, actor identity, host, and connection identity.
- Overview request counts include all retained records in the selected window. Success and nearest-rank p95 latency exclude reroutes; success is completed / non-rerouted attempts. Queue p95 excludes null waits and reroutes.
- The UI retains at most 500 records. Local runtimes append events to SQLite and retain 10,000, with paginated history queries; hosted servers currently use in-memory SQLite. The UI labels its sample scope, delivery loss, and expiration. It does not present sampled counts as full-hour totals.

## Backend gaps for a separate task

| Design feature | Missing contract | Current UI behavior |
| --- | --- | --- |
| Full-window request totals, success, latency and comparison to previous period | Server aggregates per namespace and actor class for explicit `from`/`to`, completed/failed/rejected/interrupted/rerouted counts, latency distribution/percentiles, completeness and observation coverage; matching preceding-window aggregates | Uses retained traces, labels their scope, no percent-change claim |
| Request-volume sparkline | Timestamped volume buckets and coverage for the selected interval | No fabricated trend |
| Running / dormant / asleep split | Explicit execution vs loaded-idle vs unloaded state. Current `live` means resident in memory, `dormant` means unloaded, `unknown` means no reliable report | Uses the existing Live / Dormant / Unknown semantics |
| Deployment count, filter, version badge and deployment pages | Observer-readable deployment IDs/names/revisions, actor-to-deployment mapping, current status, timestamps, artifact and runtime/SDK versions, release history | Deployment UI omitted; residency filter provided |
| WebSocket write success, writes per hour and failures | Attempted/completed/failed write counters and timestamps, actual recipient/write results, aggregation scope. Incoming WebSocket event outcomes are not outgoing write outcomes | Shows actual current connections and metadata only |
| WebSocket connection trend and lifecycle | Connection snapshots/time buckets, open/close timestamps, close reason and lifecycle events | No invented history, ages, or close reasons |
| Last updated / server freshness | Server-generated observation timestamps, host report age, completeness and stale-state indicators | Labels the client receipt time as “Last inventory update”; disconnect warnings retain prior values |
| Account/team identity and environment context | Authenticated display identity, namespace/environment display metadata and authorization scope | Uses a read-only runtime label, no mock profile |

Current actor-mailbox waiting operations are now included in inventory as `waiting: [{ id, operation }]`, in admission order. Null or omitted means reporting is unavailable; an empty array means no operations are waiting. This excludes running operations, activation, and buffers before actor admission. Updates use the existing inventory stream; they are snapshots, not a historical queue event log.

## Suggested acceptance criteria

Return unknown/unavailable measurements distinctly from numeric zero. Document which outcomes enter each denominator, timestamp units, percentile method, retention, resets, and whether aggregates are complete or sampled. Keep filters namespace-scoped and carry deployment identity on records before exposing deployment filtering. Extend the SDK observer proxy and `ObserverClient` validation alongside any new control-plane endpoints. Preserve same-origin requests and existing authentication/host checks.

Expose only data needed by the UI; do not repurpose socket handler success as delivery acknowledgement or inferred latency as queue depth. Once these contracts exist, replace the explicitly limited UI measurements with server aggregates and add the omitted visualizations.
