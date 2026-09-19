---
version: 1
slug: "src-requestobserver-tsx"
primary_target: "src/RequestObserver.tsx"
related_targets: ["src/request-history.ts", "src/client.ts", "src/observer-hooks.ts", "src/styles.css", "src/standalone.css", "src/standalone.tsx"]
---

# Requests

Mode: Operate. Audience: developers diagnosing method calls and WebSocket events.

## Task and composition

Scan newest-first requests by time, operation and actor context, transport, written outcome, Total, and Queue wait. Click a row or its Details button to inspect full operation, actor, request, host, connection, and timing values in a Radix-backed shadcn Sheet. Actor instance pages reuse this view scoped to both class and ID. Live shows the latest 500 records; Pause freezes the visible rows while collection continues, and Resume returns to current records. History searches saved events with From/To datetime fields, Actor ID, Outcome, and Search; Load older appends the next page. History includes the date in each timestamp and reports the number of saved requests shown. Total includes queue wait, actor processing, and persistence; caller network time is excluded. An absent queue duration means processing did not begin.

## Direction contract

Extend the incumbent neutral light/dark Terse developer console with existing shadcn controls, hairline table boundaries, semantic outcome color, tabular timing values, and monospace detail IDs. Preserve DESIGN.md's identity. Standalone Actors/Requests navigation stays outside the embedded component. Live/History use the existing secondary and outline buttons with aria-pressed; filters reuse shared inputs and the native select. No approved comp or new visual world accompanies this extension; preserve the existing controls and table.

## Data and recovery

Use the injected request stream and optional query method; clients without query retain the live-only workflow. Append commits before live subscribers read saved events. Preserve received rows during reconnect, resume with the opaque durable cursor, and provide Retry. Keep known delivery loss and persistence failures visible even when paused. A changed storage generation or retention reset replaces the live window and explains unavailable older events. Local SQLite retains 10,000 events, stable IDs, and replay cursors across restarts, independently of the 500-record live UI window. Hosted servers currently use in-memory SQLite without cross-replica sharing, with a replaceable append/query adapter for future durable storage.

History sends UI-owned SQL with bound time, actor class/ID, and outcome values to the query endpoint. Pagination SQL and cursors live in request-sql.ts. Instance scope stays fixed when changing time/outcome filters. Changing the search starts a fresh result set. Reject a From value after To with inline feedback. Distinguish history loading, unavailable with Retry history, and no matching saved requests with wider-range/fewer-filter guidance. Keep loaded history visible when loading another page fails and show retention-reset warnings. The live subscription stays active during History to surface delivery and persistence failures.

## Responsive behavior and access

Retain the 980px table minimum inside horizontal overflow. Live and History use 56px rows with single-line, ellipsized operation and instance columns. Full values wrap in the details sheet. Preserve right-aligned timings, wrapped toolbar actions/filters, native keyboard buttons, Escape dismissal and focus return, named tables/columns, written outcomes, alerts, visible focus, and reduced motion. No row chevrons or inline expansion.

## Finish

Validated with behavior tests and desktop/mobile browser checks using synthetic requests, including long labels, exact instance scoping, equal row heights, keyboard activation, and sheet dismissal/focus return. Prior captures in .impeccable/review/ are synthetic Requests fixtures, not Overview reference captures.
