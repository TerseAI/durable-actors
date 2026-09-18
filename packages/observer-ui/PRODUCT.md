# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Product Purpose

The little-actors observer presents actor types, residency counts, instances, and active WebSocket metadata for a namespace. It serves both the local CLI and embedded React applications.

## Users

Repository-derived assumption: developers diagnosing actor residency and connections during development or operations.

## Capabilities and Constraints

Preserve the existing read-only API, five-second polling, stale-data warnings, retry behavior, client injection, and embeddable package. Live means resident in memory according to the latest host heartbeat; dormant means unloaded; unknown means residency reporting is unavailable. Connections count active WebSockets, not people. No invented metrics or history.

The user requires React, TypeScript, Tailwind, shadcn components, and Vite. The standalone app uses Vite; the embeddable library retains its package contract.

## Brand Commitments

User-requested clean, Vercel-like UX, replacing the previous warm palette and sparse layout.

## Evidence on Hand

README.md, src/client.ts, existing behavior tests, and the user's screenshot. Additional product scope is unconfirmed; this redesign preserves the existing workflows.
