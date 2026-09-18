# @little-actors/observer

A shared React observer for the local `little-actors observe` command and embedded hosted or self-hosted applications. Lists actor types with live, dormant, and total instance counts, including deployed types with no instances. Search actor types, then select an actor to inspect its instances, residency, active WebSocket connection count, and connection metadata. Changes arrive over a live event stream, with automatic reconnection, manual retry, and visible stale-data errors.

## Embed in a React application

```tsx
import { ActorObserver, HttpObserverClient } from "@little-actors/observer"
import "@little-actors/observer/styles.css"

const client = new HttpObserverClient("/api/observe")

export function ObservePage() {
    return <ActorObserver client={client} />
}
```

React and React DOM 19 are peer dependencies supplied by the host app. The component has no router, login provider, global CSS reset, Node dependencies, or CLI-specific layout. The standalone build supplies its own header and page layout.

For project-scoped routes, use a prefix such as `/api/projects/${encodeURIComponent(projectId)}/observe`. Keep the client stable across renders (`useMemo` keyed by project ID works). Changing the client cancels the previous check and ignores late results.

## Backend contract

The default HTTP adapter subscribes to `GET <prefix>/events` with same-origin session credentials. Return `Content-Type: text/event-stream` and disable caching and proxy buffering. Each `inventory` SSE event carries a complete snapshot, including an initial snapshot on every connection. `GET <prefix>/actors` remains available for single reads. Both use this inventory shape:

```json
{
  "actors": [
    {
      "actorType": "Room",
      "live": 1,
      "dormant": 0,
      "unknown": 0,
      "instances": [
        {
          "actorId": "general",
          "status": "live",
          "connections": [
            { "id": "socket-id", "metadata": { "userId": "user-1" } }
          ]
        }
      ]
    }
  ]
}
```

The Rust control plane supplies the stream through admin-only `GET /v1/observe/events` and single reads through `GET /v1/observe/actors`. Each control plane exposes its deployment. Hosted backends must authorize the project before choosing its control plane.

The stream sends ten-second keepalive comments and emits `inventory` events only when the snapshot changes. An `error` event or a closed connection triggers reconnection with exponential backoff from one to ten seconds. The UI retains the last snapshot and marks it stale until a fresh snapshot arrives. Cancel the upstream stream when the viewer disconnects.

Live means the owning host's latest worker snapshot reports that instance in memory. Dormant means it is absent from that snapshot or its owning host session is no longer live. Unknown means the owner is live but has no fresh residency report, such as an older host. The UI shows an Unknown column only when needed. Totals include all three categories.

Each instance includes the active sockets reported by its owning actor host. A socket exposes its generated connection ID and the JSON metadata supplied when that connection was initialized. The UI reports connection count rather than people count because multiple sockets can belong to one person and the runtime does not infer identity from metadata. Closed and not-yet-activated sockets are excluded.

The worker supervisor reports residency changes immediately over the existing Rust executor connection and retains a one-second freshness heartbeat. A changed report triggers an early serialized lease renewal; after persistence, the host notifies the control plane to publish the updated inventory. Socket activation, disconnection, and metadata changes trigger early renewal too, persisting a fresh connection snapshot before notifying observers. Updated Rust hosts, control planes, and SDKs are required for this path.

Notifications are local to the receiving control-plane process. A fifteen-second reconciliation catches missed notifications, lease expiry, and storage changes made through other control-plane processes; socket snapshots are persisted with the host lease and visible to all control-plane processes. Snapshots from expired or superseded host sessions are excluded. Reports expire with the host lease, and reports older than five seconds are excluded from the next renewal. Viewing the inventory never starts a sandbox or loads actor state. Inventory reads scan ownership metadata for the deployment; a larger fleet will benefit from an indexed inventory and shared notifications across control planes.

`GET <prefix>/connection` remains available for explicit connectivity checks and returns `{ "connected": true }`. Failures return a non-2xx status. Disable caching for all observer endpoints. Redirects, malformed JSON, and invalid counts are treated as failures.

The backend verifies access, checks the control plane using server-side credentials, and returns the result. The browser never needs a control-plane admin key.

- **Local CLI:** the loopback server implements `/api/observe/events`, `/api/observe/actors`, and `/api/observe/connection`, using the API key from CLI settings.
- **Hosted:** the app backend authenticates the session and authorizes the selected organization/project before choosing the control plane. Never trust a browser-supplied target URL as authorization.
- **Self-hosted:** the same UI can be embedded behind the installation's own backend and authentication. The CLI also works against a remote self-hosted control plane using `--url`.

Existing applications can supply a custom client instead of using the HTTP adapter:

```ts
import type { ObserverClient } from "@little-actors/observer"

const client: ObserverClient = {
    async listActors(signal) {
        return backend.listActors({ signal })
    },
    async checkConnection(signal) {
        // Use your authenticated backend client; resolve on success, throw on failure.
        await backend.checkActorConnection({ signal })
    }
}
```

If you use a different response format, adapt it here. To enable streaming, also implement `watchActors(onInventory, signal): Promise<void>`: deliver full snapshots through the callback, remain pending while connected, and stop when the signal aborts. Clients without `watchActors` retain five-second polling. User sessions, project authorization, billing, and admin credentials remain outside the UI package.

## Styling

The observer uses shadcn button, table, input, and badge primitives (Radix Slot, CVA, and Tailwind) and the same semantic CSS variables: `--background`, `--foreground`, `--primary`, `--muted-foreground`, `--success`, `--danger`, `--radius`, and the other shadcn tokens. It inherits the host font. Pass `className` for placement in the host layout.

`styles.css` contains precompiled, `la:`-prefixed Tailwind utilities and observer-scoped base styles, with no global reset or semantic theme defaults. The host does not need to run Tailwind against this package. Terse already provides the required variables and its `.dark` theme class.

For a standalone or self-hosted app without those tokens, also import:

```tsx
import "@little-actors/observer/theme.css"
```

`src/theme.css` supplies a neutral light/dark palette for the standalone console. Embedded applications continue to inherit their host tokens. Components use prefixed Tailwind utilities to avoid collisions. Only import the optional theme stylesheet when the package should supply global defaults. The local CLI includes it automatically and follows the operating system's color scheme, including live changes until the user overrides it with the theme toggle.

## Builds and cross-repository use

```sh
pnpm --dir packages/observer-ui build
pnpm --dir packages/observer-ui test
pnpm --dir packages/observer-ui pack --pack-destination /tmp
```

The standalone app uses Vite, React, TypeScript, and Tailwind. To develop against a running observer, use its printed URL:

```sh
OBSERVER_API_URL=http://127.0.0.1:<observer-port> pnpm --dir packages/observer-ui dev
```

Vite proxies `/api/observe` to that URL (default: `http://127.0.0.1:4174`). No demo data is included in the app. The console supports actor search and combined instance ID/state filters; deployment totals remain unfiltered.

Vite builds both outputs from `vite.config.ts`: library mode produces the ESM package and separate scoped styles and optional theme; the default build produces the standalone browser app in `dist/standalone`. TypeScript emits the library declarations. Only the standalone app bundles React. The SDK copies those prebuilt assets into its own npm package. The `observe` command starts Vite's preview server on a loopback port and opens it in the browser; Vite is included as a runtime dependency. A Vite middleware handles the authenticated API bridge, keeping admin credentials server-side. The command requires no source checkout or build step.

To test in another repository before publishing:

```sh
pnpm --dir /path/to/hosted/frontend add /tmp/little-actors-observer-0.1.0.tgz
```

For production, publish this package independently and pin its released version in the hosted frontend. Its version is independent of the SDK/runtime release. Publishing requires access to the `@little-actors` npm scope; no publication is performed by the existing SDK release workflow. The backend and UI contract must remain compatible when either is upgraded.

The local CLI uses a workspace **development** dependency at build time. Its published runtime does not depend on an unpublished UI package or on a path to another checkout.

## Request timings

The standalone console includes a Requests view. To embed it, render `<RequestObserver client={client} />` using the same styles and `HttpObserverClient`. Its optional `watchRequests(onPage, signal)` client method consumes `<prefix>/requests/events`; proxy that stream to the admin-only `/v1/observe/requests/events` control-plane endpoint using server-side credentials. The local CLI provides this proxy automatically.

SSE `requests` events contain `{ epoch, cursor, capacity, evicted, dropped, records }`. The first event replays the available history; later events carry new records. Sequence numbers identify records within an epoch; a new epoch resets history. Each record includes `requestId`, `hostId`, `sessionId`, `actorType`, `actorId`, `kind` (`method` or `websocket`), `operation`, nullable `connectionId`, `startedAtMs`, `durationMs`, nullable `queueWaitMs`, and `outcome`. Outcomes are `completed`, `failed`, `rejected`, `rerouted`, or `interrupted`.

Total includes queue wait, actor processing, and persistence; queue wait includes the WebSocket message queue. Timings exclude the caller’s network round trip. Rows arrive after an attempt finishes. The latest 500 records are retained per control-plane process, without persistence or cross-replica sharing. Collection is best effort and known delivery losses are shown; process crashes can also lose unreported records. See [request timing semantics](../../docs/reference/cli.md#request-timings).
