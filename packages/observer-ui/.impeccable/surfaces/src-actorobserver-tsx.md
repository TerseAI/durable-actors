---
version: 1
slug: "src-actorobserver-tsx"
primary_target: "src/ActorObserver.tsx"
related_targets: ["src/styles.css","src/standalone.css","src/standalone.tsx","index.html"]
---

# Actor inventory and inline inspector

Mode: Operate. Audience: developers diagnosing namespace residency and active connections.

## Task and composition

Scan the existing total/live/dormant counts (and Unknown when present), search actor types, open an actor, narrow instances by ID and residency state, then open an instance to inspect active WebSocket IDs and JSON metadata. Keep both disclosure levels inline. Summary values derive from the full inventory, not the search results. Provide no-match recovery and distinct empty actor, instance, and connection states.

## Direction contract

The user-pinned clean, Vercel-like console supersedes the a12ca657 direction roll and the prior warm theme. Use neutral light/dark surfaces, hairline borders, compact shadcn controls, system sans, and data-only monospace. First viewport: standalone brand/Actors chrome, heading and refresh, one divided summary strip, searchable inventory. DESIGN.md records the implemented visual system.

## Data and recovery

Use the injected ObserverClient and real requests. Poll again five seconds after each request completes. Preserve previous data while refreshing and on refresh failure; show the explicit stale warning and Try again action. Initial failures explain connection/access recovery. Abort superseded requests. Live means resident according to the latest host report; dormant means unloaded; unknown preserves missing residency knowledge. Connections mean active WebSockets, not unique people. No invented history, metrics, or mutation controls.

## Embedding, responsive behavior, and access

Keep host token/font inheritance, la: utilities, and observer-scoped base CSS. Global theme defaults are optional. Standalone Vite chrome, theme toggle, skip link, and page layout remain separate. Narrow layouts stack actor search, wrap the summary when needed, retain horizontally scrollable tables, and enlarge controls. Preserve named search/state fields, semantic table headings, keyboard disclosure and focus, loading/status/alert semantics, and reduced motion.

## Finish

Reviewer disposition: ship. No material findings. Documentation describes the completed implementation; preserve the read-only package contract.
