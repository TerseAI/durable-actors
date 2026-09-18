# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Product Purpose

The little-actors observer presents actor types, residency counts, instances, active WebSocket metadata, and recent request timings for a deployment. It serves both the local CLI and embedded React applications.

## Users

Repository-derived assumption: developers diagnosing actor residency and connections during development or operations.

## Capabilities and Constraints

Preserve the read-only API, stale-data warnings, retry behavior, client injection, and embeddable package. Prefer live server-sent inventory events with automatic reconnection; retain five-second polling for custom clients without subscriptions. Live means resident in memory according to the latest persisted host report; dormant means unloaded; unknown means residency reporting is unavailable. Connections count active WebSockets, not people. Request traces report host-side total duration and queue wait for method calls and WebSocket events. Retain 500 records in process memory, show known delivery loss and expired history, and reset history on control-plane restart. No additional metrics or durable trace archive.

The user requires React, TypeScript, Tailwind, shadcn components, and Vite. The standalone app uses Vite; the embeddable library retains its package contract.

## Brand Commitments

User-requested clean, Vercel-like UX, replacing the previous warm palette and sparse layout.

## Evidence on Hand

README.md, src/client.ts, existing behavior tests, and the user's screenshot. The user authorized request traces with total duration and queue wait, preserving the existing inventory workflows.
