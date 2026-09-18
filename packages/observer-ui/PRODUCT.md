# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Product Purpose

The little-actors observer presents actor types, residency counts, instances, active WebSocket metadata, and live and saved request timings for a deployment. It serves both the local CLI and embedded React applications.

## Users

Repository-derived assumption: developers diagnosing actor residency and connections during development or operations.

## Capabilities and Constraints

Preserve the read-only API, stale-data warnings, retry behavior, client injection, and embeddable package. Prefer live server-sent inventory events with automatic reconnection; retain five-second polling for custom clients without subscriptions. Live means resident in memory according to the latest persisted host report; dormant means unloaded; unknown means residency reporting is unavailable. Connections count active WebSockets, not people. Request traces report host-side total duration and queue wait for method calls and WebSocket events. The live UI retains 500 records and shows known delivery loss, persistence failures, and retention resets. The local runtime appends events to SQLite before live subscribers read the saved events; storage retains 10,000 events independently of the live UI window. Stable event IDs and opaque resume cursors survive local restarts. History sends read-only SQL and bound parameters for inclusive time range, actor ID, and outcome, with UI-owned SQL pagination and a Load older action. The optional query client method enables History while preserving compatibility with existing live-only clients. Hosted servers currently use in-memory SQLite without cross-replica sharing; durable hosted storage must preserve the public SQL schema/dialect and enforce project isolation. The standalone Overview derives request count, success percentage, nearest-rank p95 total latency, and p95 queue wait from retained traces in a selected 15-minute, one-hour, or 24-hour window. Counts include reroutes; success and latency exclude them. Success is completed / non-rerouted attempts, and queue p95 also excludes null waits. These are retained-sample measurements, not complete interval aggregates. Missing measurements remain unavailable rather than becoming zero. Current connection counts come from inventory, and “Last inventory update” is the client receipt time.

Overview class search and residency filters lead into the existing Actors inspector. Actors and Requests retain their inspection workflows, while WebSockets lists current connections and metadata. The prior Overview implementation was UI-only work against existing APIs, with no backend or SDK contract changes and no durable trace archive in its scope. Saved request history now extends the Requests workflow; deployment metadata, identity, queue depth, fabricated trends, and outgoing-write statistics remain outside scope. [BACKEND-GAPS.md](BACKEND-GAPS.md) records the absent contracts for a separate backend task.

The user requires React, TypeScript, Tailwind, shadcn components, and Vite. The standalone app uses Vite; the embeddable library retains its package contract.

## Brand Commitments

The user approved the exact [Doop little-actors Overview](https://doop.design/c/x71to_CeX0), frame `xIP9Ls3hDj`, as the standalone visual authority for the Operate runtime console. Preserve its sidebar, compact unequal cards, grouped data table, and restrained neutral light/dark treatment while presenting only real available data. The embedded package keeps its host-aware design and contract. Use system sans and monospace stacks without external font loading under the current Content Security Policy.

## Evidence on Hand

README.md, src/client.ts, src/overview-data.ts, existing behavior tests, and the approved Doop frame. The approved Overview reference is recorded in .impeccable/review/approved-doop.png. The current .impeccable/review/{desktop,mobile}.png captures show synthetic Requests fixtures, not the original Overview implementation. The prior standalone Overview design implementation used existing APIs only; the Requests history extension preserves the incumbent console and inspection workflow.
