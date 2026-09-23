# Durable Actors Observer

React 19 components for embedding Durable Actors observability in a hosted app.

```tsx
import { HttpObserverClient, Overview, WebSocketObserver } from "durable-actors-observer"
import "durable-actors-observer/styles.css"
import "durable-actors-observer/theme.css"

const projectId = "my-project"
const client = new HttpObserverClient(`/api/projects/${encodeURIComponent(projectId)}/observe`)

export function Observability() {
    return (
        <>
            <Overview client={client} onSelectActor={actorName => console.log(actorName)} />
            <WebSocketObserver client={client} />
        </>
    )
}
```

Import `styles.css` once. The optional `theme.css` supplies light and dark theme tokens; omit it when the host supplies those tokens. Apply the `dark` class to an ancestor for dark mode.

The package exports `ActorObserver`, `RequestObserver`, `Overview`, `WebSocketObserver`, `ConsoleApp`, `SocketTimeline`, `TimeRangePicker`, and `FilterCombobox`, along with their named props types. It also exports the shared badge, button, calendar, command, input, popover, sheet, and table components from the package root. Use React's `ComponentProps<typeof Button>` (and equivalent) for primitive props.

Pass your own `ObserverClient` to connect these views to a hosted backend, or configure `HttpObserverClient` with your API prefix and fetch implementation. React remains a peer dependency. `ConsoleApp` provides the complete navigation shell and requires a `toggleTheme` callback; individual views can be placed inside your own navigation.

Control-plane endpoints require a project in the path: `/v1/projects/{project_id}/observe/...`. The hosted backend should authorize access to that project and proxy the routes using its server-side admin credential. The CLI's `/api/observe` proxy uses its configured `DURABLE_ACTORS_PROJECT_ID`. Request traces include a required `projectId`; older stored traces without one are excluded from project results.

The observer version follows the SDK and runtime version. Run `pnpm release:prepare <version>` at the repository root before publishing a GitHub release. Release preflight checks all versions, and the workflow publishes the observer before the SDK. The SDK's `workspace:*` dependency becomes the exact observer version when packed.
