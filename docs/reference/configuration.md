# Environment variables

The CLI reads `.env` in the current directory. Exported variables take precedence. Application backends must load their own environment; containers can use `--env-file`.

## Application connection

Used by backend clients, `generate --remote`, and `observe`. Local startup also uses the project ID and API key.

| Variable                           | Default                                             | Meaning                                                                                                                     |
| ---------------------------------- | --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost; required remotely             | Actor project ID; use the same value in the server registration and backend.                                                |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                             | HTTP(S) server origin. Paths, query strings, fragments, and embedded credentials are not allowed.                           |
| `DURABLE_ACTORS_SECRET`            | Unset                                               | Optional shared secret. Set the same value on the server and backend to enable authentication. Keep it out of browser code. |

Local CLI commands and backend clients need no connection settings with the defaults. Connections to localhost (`localhost`, `127.0.0.1`, and `[::1]`) default to project `local`. Remote connections require an explicit project ID. If you override the project or port, use matching settings in your backend.

Authentication is disabled when `DURABLE_ACTORS_SECRET` is unset on the server, locally or remotely. Set it on both the server and your backend to test or enable authentication. Clients omit the authorization header when no secret is configured. Leave the variable unset to disable authentication; empty values and surrounding whitespace are invalid on the server.

## Local development

Used by `dev`.

| Variable                    | Default                     | Meaning                                                                                                               |
| --------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT`    | `.`                         | Actor project directory.                                                                                              |
| `DURABLE_ACTORS_ENTRYPOINT` | `src/actors.ts`             | Actor source file, relative to the project.                                                                           |
| `DURABLE_ACTORS_PORT`       | `7100`                      | Listening port; `0` selects a free port. `--port` overrides it for one run.                                           |
| `DURABLE_ACTORS_DATA_DIR`   | `<project>/.durable-actors` | Persistent local state directory.                                                                                     |
| `DURABLE_ACTORS_STORAGE`    | `local`                     | `local` for file storage or `gcs` for a GCS bucket. GCS also requires `DURABLE_ACTORS_BUCKET` and Google credentials. |

## Server hosting

The server requires a `DURABLE_ACTORS_CONTROL_PLANE_URL` reachable by its actor hosts and clients, which may be on a private network.

`DURABLE_ACTORS_SECRET` is optional for `durable-actors start`, the native executable, and the container. When the listening address is not localhost and no secret is configured, startup warns that anyone who can reach the server can access its API, then continues. The SDK also accepts `DURABLE_ACTORS_API_KEY`, with `DURABLE_ACTORS_SECRET` taking precedence. Internal actor and storage credentials are still required and are managed separately.

| Variable                            | Default                                                 | Meaning                                                                            |
| ----------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROCESS_ROLE`       | CLI `start`: `control_plane`; native executable: `host` | Set to `control_plane` when running the server container.                          |
| `DURABLE_ACTORS_CONTROL_PLANE_BIND` | `127.0.0.1:7100`                                        | Listening address. Use `0.0.0.0:7100` inside a container.                          |
| `DURABLE_ACTORS_POSTGRES_URL`       | Required                                                | PostgreSQL connection URL. The database user must be able to run migrations.       |
| `DURABLE_ACTORS_BUCKET`             | Required                                                | GCS bucket name without `gs://`. Also required for local GCS storage.              |
| `GOOGLE_APPLICATION_CREDENTIALS`    | Google Application Default Credentials                  | Path to a service-account credentials file; omit with an attached Google identity. |
| `DURABLE_ACTORS_SANDBOX_PROVIDER`   | Required                                                | Supported value: `modal`.                                                          |
| `DURABLE_ACTORS_RUNTIME_IMAGE`      | Required                                                | Shared Modal runtime image ID (`im-...`), matching the SDK version.                |
| `MODAL_TOKEN_ID`                    | Required                                                | Token ID for the Modal workspace containing your images.                           |
| `MODAL_TOKEN_SECRET`                | Required                                                | Modal token secret.                                                                |
| `DURABLE_ACTORS_JWT_SIGNING_KEY`    | Required; generated for local development               | Base64-encoded Ed25519 PKCS#8 signing key. Reuse across restarts.                  |

When importing the runtime image into Modal, clear its Docker entrypoint with `modal.Image.from_registry(..., add_python="3.12").entrypoint([])` so the provider can run its build, actor, and replica commands.

## Advanced settings

### Capacity and placement

| Variable                                    | Default              | Meaning                                                                                                                                                                                                         |
| ------------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS`      | `10000`              | Actor idle time before eviction; 1–86400000 ms. Applies locally too. Method calls and WebSocket messages reset the timer; running handlers defer eviction. Open WebSockets retain the host and connections, but not the actor instance. Without open sockets, an idle host shuts down. |
| `DURABLE_ACTORS_HOST_STARTUP_MS`            | `10000`              | Positive actor-host startup timeout in milliseconds.                                                                                                                                                            |
| `DURABLE_ACTORS_SPARE_IDLE`                 | `5`                  | Ready sandboxes per role, image, region, and resource configuration; 0–32. Zero creates hosts on demand. Servers sharing PostgreSQL must use matching pool settings. Named Modal secrets bypass the actor pool. |
| `DURABLE_ACTORS_SPARE_REGIONS`              | `north-america-east` | Comma-separated regions for ready actor hosts.                                                                                                                                                                  |
| `DURABLE_ACTORS_SPARE_TTL_SECONDS`          | `600`                | Unassigned host lifetime; 30–3600 seconds.                                                                                                                                                                      |
| `DURABLE_ACTORS_HOST_CPU_MILLIS`            | `1000`               | Actor CPU request and cap; 100–64000 millicores.                                                                                                                                                                |
| `DURABLE_ACTORS_HOST_MEMORY_MIB`            | `1024`               | Actor memory request and cap; 128–262144 MiB.                                                                                                                                                                   |
| `DURABLE_ACTORS_REPLICA_REGIONS`            | `[]`                 | JSON list of up to eight replica regions; duplicates allowed. Empty uses object storage only.                                                                                                                   |
| `DURABLE_ACTORS_REGION`                     | Unset                | Default region for new actors. Without a decorator region override, explicit assignments must match it; existing actors keep their saved home.                                                                 |
| `DURABLE_ACTORS_HOME_REGION`                | Unset                | Region requested by a trusted backend. Omit to use the actor's saved home or the server default.                                                                                                                |

### Per-actor sandbox overrides

```ts
import { Actor, Sandbox } from "durable-actors"

@Sandbox({
    cpu: 2,
    memoryMiB: 2048,
    regions: ["canada"],
    idleTimeoutMs: 60_000,
})
export class CustomerAgent extends Actor {}
```

| Option | Meaning | Default when omitted |
| --- | --- | --- |
| `cpu` | CPU request and cap in cores; 0.1–64 in increments of 0.001. | `DURABLE_ACTORS_HOST_CPU_MILLIS` divided by 1000; normally 1. |
| `memoryMiB` | Memory request and cap; integer from 128–262144 MiB. | `DURABLE_ACTORS_HOST_MEMORY_MIB`; normally 1024. |
| `regions` | Nonempty list of unique allowed compute regions. Order is not a preference. | Existing placement and server defaults. |
| `idleTimeoutMs` | Inactivity before eviction; integer from 1–86400000 ms. | `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS`; normally 10000 (10 seconds). |

Supported regions: `canada`, `north-america-east`, `north-america-central`, `north-america-south`, `north-america-west`, `europe-west`, `asia-southeast`.

### Actor log exports

Set `logExport` per project through `PUT /v1/projects/{project_id}/deployment`. Include the project's existing deployment fields:

```json
{
    "imageRef": "im-customer",
    "workingDirectory": "/project",
    "actorEntrypoint": "src/actors.ts",
    "secretRefs": ["customer-log-credentials"],
    "logExport": {
        "endpoint": "https://collector.example/v1/logs",
        "headersEnv": "ACTOR_LOG_HEADERS"
    }
}
```

| Option | Meaning |
| --- | --- |
| `endpoint` | Full OTLP/HTTP protobuf logs URL, including its path. HTTP(S) only; no credentials, query, or fragment. Redirects are disabled. |
| `headersEnv` | Optional actor environment variable containing a JSON object of authentication headers. |

Store `ACTOR_LOG_HEADERS` in the named Modal secret, for example `{"Authorization":"Bearer <token>"}`. Only the variable name is saved in deployment configuration. Local development reads the variable from its environment. Omit `logExport` or set it to `null` to disable exports; include it in subsequent deployment updates to retain it.

| Provider | Endpoint and authentication |
| --- | --- |
| [Datadog](https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest/logs/) | Your site's OTLP logs intake URL; `dd-api-key` header. |
| [Sentry](https://docs.sentry.io/concepts/otlp/direct/logs/) | OTLP logs URL from Project Settings → Client Keys; `x-sentry-auth: sentry sentry_key=<public-key>`. Direct OTLP logs are in open beta. |
| [PostHog](https://posthog.com/docs/logs/installation/rust) | `<project-api-host>/i/v1/logs`; `Authorization: Bearer <phc_project_token>`. |
| OpenTelemetry Collector | Its HTTP logs endpoint, normally `/v1/logs`; authentication depends on the collector. |

Exports include actor stdout/stderr and host runtime events, tagged with project, actor, host, and region. On Modal, actor process output is suppressed even when no export is configured; no stdout/stderr copy is saved in Modal logs or a host log file. Local output remains unchanged unless exporting. Delivery uses a bounded memory queue and flushes on graceful shutdown; outages, queue overflow, and abrupt termination can lose logs.

Control-plane, replica, and build logs are separate and are not exported. Control-plane logs currently include actor identifiers and error text, so they may contain customer data. For residency requirements, configure a suitable collector/provider location and place those other services and logs accordingly.

### Authentication and callbacks

| Variable                                | Default                              | Meaning                                                                                |
| --------------------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_JWT_KEY_ID`             | `primary`                            | Signing key identifier.                                                                |
| `DURABLE_ACTORS_JWT_ISSUER`             | `durable-actors-control-plane`       | Token issuer.                                                                          |
| `DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE` | `durable-actors-authority`           | Server authentication audience.                                                        |
| `DURABLE_ACTORS_INVOKE_JWT_AUDIENCE`    | `durable-actors-invoke`              | Actor-call audience.                                                                   |
| `DURABLE_ACTORS_SOCKET_JWT_AUDIENCE`    | `durable-actors-authority:websocket` | Socket audience for manual hosts; must match the authority audience plus `:websocket`. |
| `DURABLE_ACTORS_JWT_MAX_TTL_SECONDS`    | `86400`                              | Positive maximum credential lifetime in seconds.                                       |
| `DURABLE_ACTORS_SOCKET_EVENT_URL`       | Disabled                             | Incoming-message callback URL. Callbacks send the shared secret when configured and omit authorization otherwise; see [OpenAPI](openapi.yaml).                            |

### Diagnostics and runtime overrides

| Variable                         | Default                   | Meaning                                                                                        |
| -------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------- |
| `RUST_LOG`                       | `info`                    | Runtime log filter, such as `warn` or `debug`; the packaged container supplies its own filter. |
| `DURABLE_ACTORS_TELEMETRY`       | Disabled                  | Set to `1` to enable SDK invocation telemetry on standard error; unset or `0` keeps it disabled. |
| `DURABLE_ACTORS_BINARY`          | Downloaded runtime        | Use an existing native executable. Relative paths resolve from the working directory.          |
| `DURABLE_ACTORS_CACHE_DIR`       | `~/.cache/durable-actors` | Runtime download cache; ignored when `DURABLE_ACTORS_BINARY` is set.                           |
| `DURABLE_ACTORS_SANDBOX_COMMAND` | `durable-actors-modal-go` | Provider executable for a custom runtime distribution.                                         |
