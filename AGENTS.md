# Terse engineering conventions

Rules for working in this repo. Keep changes minimal and idiomatic.

## Core rules

1. **Minimize comments.** Add one only when a choice is non-obvious, odd, or a deliberate compromise whose rationale must remain beside the code.

2. **Prefer a library** over building it yourself for common problems.

3. **Use dependency injection at effectful boundaries.** Code against a narrow interface when an implementation must be replaceable, and inject dependencies through constructors. Do not add interfaces around pure helpers.

4. **Follow the step-down rule.** Present public orchestration first, followed by progressively lower-level helpers in call order.

5. **Keep methods and functions short.** Split work into named operations when a function mixes responsibilities or abstraction levels.

6. **Follow SOLID principles**

## Observability data

Everything the observer dashboard shows takes one path. Add new data the same way:

1. **Emit an OTLP record.** Requests are host spans. Everything else is a log record named `durable_actors.<domain>.<event>`: implement `PlatformEvent` for the event and record it through `PlatformEvents<E>`. Never write ClickHouse or Redis from application code.
2. **Read it from ClickHouse.** The collector writes `platform_events`; add a `.sql` query in `src/request_traces/persistence/clickhouse/` (a materialized view only when it needs aggregating), a `TraceQueries` method, and an `observe/<view>` route.
3. **Decode it for the live stream.** Add a case in `src/control_plane/observe_events.rs`; `observe/events` forwards it from the project's Redis channel.
4. **Show it with `useLiveView`.** Add the event type to `packages/observer-ui/src/types/Events.ts` and a read plus apply function in `lib/views/`. Views read the store when they open and apply events afterwards; no polling.
