# Environment variables

The CLI reads `.env` in the current directory. Exported variables take precedence. Application backends must load their own environment; containers can use `--env-file`.

## Application connection

Used by backend clients, `generate --remote`, and `observe`. Local startup also uses the project ID and API key.

| Variable                           | Default                                 | Description                                                                                                                     |
| ---------------------------------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost; required remotely | Actor project ID; use the same value in the server registration and backend.                                                |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                 | HTTP(S) server origin. Paths, query strings, fragments, and embedded credentials are not allowed.                           |
| `DURABLE_ACTORS_SECRET`            | Unset                                   | Optional shared secret. Set the same value on the server and backend to enable authentication. Keep it out of browser code. |

Local CLI commands and backend clients need no connection settings with the defaults. Connections to localhost (`localhost`, `127.0.0.1`, and `[::1]`) default to project `local`. Remote connections require an explicit project ID. If you override the project or port, use matching settings in your backend.

Authentication is disabled when `DURABLE_ACTORS_SECRET` is unset on the server, locally or remotely. Set it on both the server and your backend to test or enable authentication. Clients omit the authorization header when no secret is configured. Leave the variable unset to disable authentication; empty values and surrounding whitespace are invalid on the server.

## Local development

Used by `dev`.

| Variable                    | Default                     | Description                                                                                                               |
| --------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT`    | `.`                         | Actor project directory.                                                                                              |
| `DURABLE_ACTORS_ENTRYPOINT` | `src/actors.ts`             | Actor source file, relative to the project.                                                                           |
| `DURABLE_ACTORS_PORT`       | `7100`                      | Listening port; `0` selects a free port. `--port` overrides it for one run.                                           |
| `DURABLE_ACTORS_DATA_DIR`   | `<project>/.durable-actors` | Persistent local state directory.                                                                                     |
| `DURABLE_ACTORS_STORAGE`    | `local`                     | `local` for file storage or `gcs` for a GCS bucket. GCS also requires `DURABLE_ACTORS_BUCKET` and Google credentials. |

## Kubernetes hosting

Use the [Helm chart](../../charts/terse/README.md) for GKE Agent Substrate. It runs the control plane, a WebSocket connection gateway, and shared sandbox workers. The chart can place WebSocket gateways in a separate deployment. The chart sets these runtime variables:

| Variable                                    | Description                                                                                                                                                                                                                                             |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROCESS_ROLE`               | `control_plane` for the server; `warm` for the generic snapshot runtime.                                                                                                                                                                             |
| `DURABLE_ACTORS_CONTROL_PLANE_BIND`         | Listen address, `0.0.0.0:7100` in the chart.                                                                                                                                                                                                        |
| `DURABLE_ACTORS_CONTROL_PLANE_URL`          | Private Kubernetes Service origin reachable from sandboxes.                                                                                                                                                                                         |
| `DURABLE_ACTORS_GATEWAY_ROUTE`              | Required private HTTP origin of this gateway pod; the chart uses the pod IP.                                                                                                                                                                        |
| `DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS` | Whether this process can own socket rooms; defaults to `true`. The chart disables it on ordinary control-plane replicas when dedicated gateways are enabled.                                                                                        |
| `DURABLE_ACTORS_PUBLIC_URL`                 | Public HTTPS origin for client invocation and socket routing.                                                                                                                                                                                       |
| `DURABLE_ACTORS_POSTGRES_URL`               | Registry and trace database; migrations required.                                                                                                                                                                                |
| `DURABLE_ACTORS_BUCKET`                     | Standard GCS authority bucket for CAS ownership and leases.                                                                                                                                                                                         |
| `DURABLE_ACTORS_ARTIFACT_BUCKET`            | Immutable compiled customer code.                                                                                                                                                                                                                   |
| `DURABLE_ACTORS_ARCHIVE_BUCKET`             | Permanent Standard GCS bucket for manifests, checkpoints, and archived log segments.                                                                                                                                                                |
| `DURABLE_ACTORS_RAPID_BUCKETS`              | JSON array of exactly two `{ "bucket": "name", "zone": "us-west4-a" }` placements in distinct Rapid zones. Both durable flushes are required for acknowledgment.                                                                                    |
| `DURABLE_ACTORS_RUNTIME_IMAGE`              | Shared runtime OCI image pinned by SHA-256 digest.                                                                                                                                                                                                  |
| `DURABLE_ACTORS_JWT_SIGNING_KEY`            | Shared base64 Ed25519 PKCS#8 key; stable across restarts.                                                                                                                                                                                           |
| `GOOGLE_APPLICATION_CREDENTIALS`            | Optional ADC file; use Workload Identity on GKE.                                                                                                                                                                                                    |

## Advanced settings

### WebSockets

| Variable                                | Default | Description                                                                                                                   |
| --------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS` | `32768` | Per-actor connection limit; 1–32,768, including pending connect handlers. Actual capacity depends on workload and gateway resources. |

### Capacity and placement

Actor resource limits come from `@Sandbox` (default 1 CPU and 256 MiB). Resource shapes use the same WorkerPool; deployment registration prepares snapshots containing customer code before activation. Worker capacity must cover concurrent reservations and snapshot preparation. The chart exposes capacity through `substrate.worker`.

| Variable | Description |
| --- | --- |
| `DURABLE_ACTORS_SUBSTRATE_ENDPOINT` | Private TLS gRPC API origin. |
| `DURABLE_ACTORS_SUBSTRATE_ROUTER` | Private HTTP actor-router origin. |
| `DURABLE_ACTORS_SUBSTRATE_ATESPACE` | Namespace for this runtime's Substrate actors and templates. |
| `DURABLE_ACTORS_SUBSTRATE_REGIONS` | JSON array of configured canonical regions. |
| `DURABLE_ACTORS_SUBSTRATE_WORKER_LABELS` | JSON map selecting worker pools; region is added automatically. |
| `DURABLE_ACTORS_SUBSTRATE_SNAPSHOTS` | Private `gs://` snapshot prefix ending in `/`. |
| `DURABLE_ACTORS_SUBSTRATE_SANDBOX_CONFIG` | Installed Substrate sandbox configuration, normally `gvisor-default`. |
| `DURABLE_ACTORS_SUBSTRATE_TOKEN_FILE` | Projected service-account token, reread for each API call. |
| `DURABLE_ACTORS_SUBSTRATE_TRUST_BUNDLE` | Projected CA PEM file for API TLS. |
| `DURABLE_ACTORS_SUBSTRATE_EGRESS_CIDRS` | JSON array of permitted customer destinations; control-plane Service IP is added automatically. |
| `DURABLE_ACTORS_SECRETS_NAMESPACE` | Namespace containing labeled customer Secrets. |

`DURABLE_ACTORS_SANDBOX_IDENTITY_FILE` supplies the current Substrate actor UID through a SystemInfo volume. Assignment verification rereads it for every request and rejects a missing or empty identity. `DURABLE_ACTORS_ASSIGNMENT_PUBLIC_KEYS` contains the public JWKS used to verify assignment capabilities; the snapshot does not contain the signing key.

| Variable                              | Default | Description                                                                                                                                                                                                                                                                                     |
| ------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS` | `10000` | Actor idle time before eviction; 1–86400000 ms. Applies locally too. Method calls and WebSocket handlers reset the timer; running handlers defer eviction. Open WebSockets remain at the gateway and do not keep the sandbox alive. Automatic gateway replies do not reset actor idle time. |
| `DURABLE_ACTORS_REGION`               | Unset   | Default region for new actors. Without a decorator region override, explicit assignments must match it; existing actors keep their saved home.                                                                                                                                              |
| `DURABLE_ACTORS_HOME_REGION`          | Unset   | Region requested by a trusted backend. Omit to use the actor's saved home or the server default.                                                                                                                                                                                            |

### Per-actor sandbox overrides

```ts
import { Actor, Sandbox } from "durable-actors"

@Sandbox({
    cpu: 2,
    memoryMiB: 2048,
    regions: ["canada"],
    idleTimeoutMs: 60_000
})
export class CustomerAgent extends Actor {}
```

| Option          | Description                                                                     | Default when omitted                                                |
| --------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `cpu`           | CPU request and cap in cores; 0.1–64 in increments of 0.001.                | 1 CPU. |
| `memoryMiB`     | Memory request and cap; integer from 128–262144 MiB.                        | 256 MiB. |
| `regions`       | Nonempty list of unique allowed compute regions. Order is not a preference. | Existing placement and server defaults.                             |
| `idleTimeoutMs` | Inactivity before eviction; integer from 1–86400000 ms.                     | `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS`; normally 10000 (10 seconds). |

Supported regions: `canada`, `north-america-east`, `north-america-central`, `north-america-south`, `north-america-west`, `europe-west`, `asia-southeast`.

### Authentication and callbacks

| Variable                                | Default                        | Description                                                                                                                                        |
| --------------------------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_JWT_KEY_ID`             | `primary`                      | Signing key identifier.                                                                                                                        |
| `DURABLE_ACTORS_JWT_ISSUER`             | `durable-actors-control-plane` | Token issuer.                                                                                                                                  |
| `DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE` | `durable-actors-authority`     | Server authentication audience.                                                                                                                |
| `DURABLE_ACTORS_INVOKE_JWT_AUDIENCE`    | `durable-actors-invoke`        | Actor-call audience.                                                                                                                           |
| `DURABLE_ACTORS_JWT_MAX_TTL_SECONDS`    | `86400`                        | Positive maximum credential lifetime in seconds.                                                                                               |
| `DURABLE_ACTORS_SOCKET_EVENT_URL`       | Disabled                       | Incoming-message callback URL. Callbacks send the shared secret when configured and omit authorization otherwise; see [OpenAPI](openapi.yaml). |

### Diagnostics and runtime overrides

| Variable                   | Default                   | Description                                                                                          |
| -------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------ |
| `RUST_LOG`                 | `info`                    | Runtime log filter, such as `warn` or `debug`; the packaged container supplies its own filter.   |
| `DURABLE_ACTORS_TELEMETRY` | Disabled                  | Set to `1` to enable SDK invocation telemetry on standard error; unset or `0` keeps it disabled. |
| `DURABLE_ACTORS_BINARY`    | Downloaded runtime        | Use an existing native executable. Relative paths resolve from the working directory.            |
| `DURABLE_ACTORS_CACHE_DIR` | `~/.cache/durable-actors` | Runtime download cache; ignored when `DURABLE_ACTORS_BINARY` is set.                             |
