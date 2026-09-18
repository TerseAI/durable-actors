---
version: 1
slug: "src-requestobserver-tsx"
primary_target: "src/RequestObserver.tsx"
related_targets: ["src/styles.css", "src/standalone.css", "src/standalone.tsx"]
---

# Recent requests

Mode: Operate. Audience: developers diagnosing method calls and WebSocket events.

## Task and composition

Scan newest-first requests by time, operation and actor context, transport, written outcome, Total, and Queue wait. Open an operation to inspect Request ID, Host, and optional Connection inline. Pause freezes the visible history while collection continues; Resume returns to current records. Total includes queue wait, actor processing, and persistence; caller network time is excluded. An absent queue duration means processing did not begin.

## Direction contract

Extend the incumbent neutral Vercel-like light/dark console with existing shadcn controls, hairline table boundaries, semantic outcome color, tabular timing values, and monospace detail IDs. Preserve DESIGN.md's identity. Standalone Actors/Requests navigation stays outside the embedded component. No new visual system or additional metrics.

## Data and recovery

Use the injected request stream. Preserve received rows during reconnect and provide Retry. Keep known delivery loss visible even when paused. Distinguish connecting, unavailable, and empty history states. Explain the latest 500-record in-memory window, expired records, and control-plane restart reset; this is not a durable archive.

## Responsive behavior and access

Retain the 720px table minimum inside horizontal overflow, wrapping operation/actor labels and detail IDs, right-aligned timing columns, and wrapped toolbar actions on narrow screens. Preserve keyboard disclosure, aria-expanded, named table and column scopes, written outcome labels, alerts, visible focus, and reduced motion.

## Finish

Reviewer disposition: ship. No material findings. Documentation records the implemented local extension.
