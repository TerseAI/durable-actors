# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Product Purpose

The little-actors observer presents actor types, residency counts, instances, active WebSocket metadata, and recent request timings for a deployment. It serves both the local CLI and embedded React applications.

## Users

Repository-derived assumption: developers diagnosing actor residency and connections during development or operations.

## Capabilities and Constraints

Preserve the read-only API, stale-data warnings, retry behavior, client injection, and embeddable package. Prefer live server-sent inventory events with automatic reconnection; retain five-second polling for custom clients without subscriptions. Live means resident in memory according to the latest persisted host report; dormant means unloaded; unknown means residency reporting is unavailable. Connections count active WebSockets, not people. Request traces report host-side total duration and queue wait for method calls and WebSocket events. Retain 500 records in process memory, show known delivery loss and expired history, and reset history on control-plane restart. The standalone Overview derives request count, success percentage, nearest-rank p95 total latency, and p95 queue wait from retained traces in a selected 15-minute, one-hour, or 24-hour window. Counts include reroutes; success and latency exclude them. Success is completed / non-rerouted attempts, and queue p95 also excludes null waits. These are retained-sample measurements, not complete interval aggregates. Missing measurements remain unavailable rather than becoming zero. Current connection counts come from inventory, and “Last inventory update” is the client receipt time.

Overview class search and residency filters lead into the existing Actors inspector. Actors and Requests retain their inspection workflows, while WebSockets lists current connections and metadata. This is UI-only work against existing APIs; no backend or SDK contract changes. No durable trace archive, deployment metadata, identity, queue depth, fabricated trends, or outgoing-write statistics. [BACKEND-GAPS.md](BACKEND-GAPS.md) records the absent contracts for a separate backend task.

The user requires React, TypeScript, Tailwind, shadcn components, and Vite. The standalone app uses Vite; the embeddable library retains its package contract.

## Brand Commitments

The user approved the exact [Doop little-actors Overview](https://doop.design/c/x71to_CeX0), frame `xIP9Ls3hDj`, as the standalone visual authority for the Operate runtime console. Preserve its sidebar, compact unequal cards, grouped data table, and restrained neutral light/dark treatment while presenting only real available data. The embedded package keeps its host-aware design and contract. Use system sans and monospace stacks without external font loading under the current Content Security Policy.

## Evidence on Hand

README.md, src/client.ts, src/overview-data.ts, existing behavior tests, and the approved Doop frame. The approved reference and implementation screenshots are recorded in .impeccable/review/{approved-doop,desktop,mobile}.png. The user authorized the standalone design implementation with existing APIs only, preserving inventory and request inspection workflows.
