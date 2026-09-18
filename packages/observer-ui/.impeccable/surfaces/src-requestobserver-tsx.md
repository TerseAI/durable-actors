---
version: 1
slug: "src-requestobserver-tsx"
primary_target: "src/RequestObserver.tsx"
related_targets: ["src/request-history.ts", "src/client.ts", "src/observer-hooks.ts", "src/styles.css", "src/standalone.css", "src/standalone.tsx"]
---

# Requests

Mode: Operate. Audience: developers diagnosing method calls and WebSocket events.

## Task and composition

Scan newest-first requests by time, operation and actor context, transport, written outcome, Total, and Queue wait. Open an operation to inspect Request ID, Host, and optional Connection inline. Live shows the latest 500 records; Pause freezes the visible rows while collection continues, and Resume returns to current records. History searches saved events with From/To datetime fields, Actor ID, Outcome, and Search; Load older appends the next page. History includes the date in each timestamp and reports the number of saved requests shown. Total includes queue wait, actor processing, and persistence; caller network time is excluded. An absent queue duration means processing did not begin.

## Direction contract

Extend the incumbent neutral light/dark Terse developer console with existing shadcn controls, hairline table boundaries, semantic outcome color, tabular timing values, and monospace detail IDs. Preserve DESIGN.md's identity. Standalone Actors/Requests navigation stays outside the embedded component. Live/History use the existing secondary and outline buttons with aria-pressed; filters reuse shared inputs and the native select. No approved comp or new visual world accompanies this extension; preserve the existing controls and table.

## Data and recovery

Use the injected request stream and optional query method; clients without query retain the live-only workflow. Append commits before live subscribers read saved events. Preserve received rows during reconnect, resume with the opaque durable cursor, and provide Retry. Keep known delivery loss and persistence failures visible even when paused. A changed storage generation or retention reset replaces the live window and explains unavailable older events. Local SQLite retains 10,000 events, stable IDs, and replay cursors across restarts, independently of the 500-record live UI window. Hosted servers currently use in-memory SQLite without cross-replica sharing, with a replaceable append/query adapter for future durable storage.

History sends inclusive time, actor ID, and outcome filters to the backend and follows opaque nextCursor pagination; the UI does not query SQLite directly. Changing the search starts a fresh result set. Reject a From value after To with inline feedback. Distinguish history loading, unavailable with Retry history, and no matching saved requests with wider-range/fewer-filter guidance. Keep loaded history visible when loading another page fails and show retention-reset warnings. The live subscription is suspended while viewing History.

## Responsive behavior and access

Retain the 720px table minimum inside horizontal overflow, wrapping operation/actor labels and detail IDs, right-aligned timing columns, and wrapped toolbar actions and filter fields on narrow screens. Preserve keyboard disclosure, aria-expanded, named table and column scopes, written outcome labels, alerts, visible focus, and reduced motion.

## Finish

Reviewer disposition: ship. No material visual findings. Documentation records the implemented saved-history extension. Current desktop/mobile captures in .impeccable/review/ use synthetic Requests fixtures; they are not original Overview reference captures.
