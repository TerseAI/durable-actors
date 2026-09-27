# Cold activation timings

Production JSON logs at `info` separate sandbox assignment, host preparation, and
state recovery. Match `actor_host_provisioning.host_id` to
`actor_host_startup.host_id` and `actor_activation_phase.span.host_id`. The
activation span also includes `session_id`, `project_id`, `actor_name`, and
`actor_id`. Host events are emitted to the sandbox's stdout.

## Provider and host

`actor_host_provisioning` includes `modal_code_mount_started_at_ms`,
`modal_code_mounted_at_ms`, `modal_assignment_started_at_ms`, and
`modal_assignment_completed_at_ms`. Each is relative to the provider operation's
start. Subtract each matching start/end pair for its duration. Code mounting and
assignment overlap; do not add their durations.

`actor_host_startup` reports milestones relative to host configuration loading:

| Field | Work completed |
| --- | --- |
| `control_plane_ready_at_ms` | Control-plane connection and host listener |
| `storage_ready_at_ms` | Storage initialization, ownership recovery, and lease registration |
| `executor_ready_at_ms` | Customer module available, loaded, and executor connected |
| `lease_registered_at_ms` | Both storage and executor preparation joined |
| `executor_notified_at_ms` | Actor hydrated and readiness sent |

Storage and executor preparation run concurrently. Subtract
`control_plane_ready_at_ms` from each readiness milestone to estimate each branch's
elapsed time. A failed startup can have completion timestamps for failed branches;
check `outcome` before treating a timestamp as successful readiness.

## Storage phases

`actor_activation_phase` records a duration in `duration_ms` and an `outcome` of
`completed`, `failed`, or `cancelled`. These events are scoped to ownership
activation; ordinary reads and writes do not enable them. No state contents,
results, credentials, signed URLs, or raw error payloads are included.

The top-level phases are `ownership_read` (existing actors), `session_recovery`,
`latest_snapshot`, and `ownership_write`. `activation_total` includes all of them.

Within `session_recovery`, events distinguish `recovery_session_read`,
`recovery_session_claim`, `recovery_replicas_seal`, `recovery_snapshots_restore`,
and `recovery_finish`. The finish step further records `recovery_finish_read` and
`recovery_finish_write`. Already-sealed sessions skip sealing and restoration.

Within `latest_snapshot`, events distinguish `snapshot_list`, `snapshot_read`,
`replica_members_read`, `replica_heads`, and, when needed, `snapshot_recover`.
Snapshot recovery reports `recovery_snapshot_read`, `recovery_replica_read`, and
`recovery_snapshot_write` where applicable. Retries and multiple snapshots can
produce repeated phases.

Parent durations include their children. Compare top-level phases first, then
expand the slow phase; summing every event would double-count nested work.
