# Crons

All schedules use **UTC** and [Cloudflare's five-field syntax](https://developers.cloudflare.com/workers/configuration/cron-triggers/), validated by Saffron. Numeric weekdays run from 1 (Sunday) to 7 (Saturday). Seconds, timezone prefixes, and aliases such as `@daily` are unsupported.

Decorate any public method accepting `CronEvent` and returning nothing. An actor can have multiple cron methods; stack decorators for multiple schedules on one method.

```ts
import { Actor, Cron, Persisted, type CronEvent } from "durable-actors"

export class Jobs extends Actor {
    @Persisted lastRun = 0

    @Cron("*/5 * * * *", { retries: 3 })
    async refresh(event: CronEvent): Promise<void> {
        if (event.scheduledTime <= this.lastRun) return
        this.lastRun = event.scheduledTime
    }
}
```

```python
from durable_actors import Actor, CronEvent, cron

class Jobs(Actor):
    last_run: int = 0

    @cron("*/5 * * * *", retries=3)
    def refresh(self, event: CronEvent) -> None:
        if event.scheduled_time <= self.last_run:
            return
        self.last_run = event.scheduled_time
```

`event.cron` is the expression. `scheduledTime` (`scheduled_time` in Python) is the original scheduled Unix time in **milliseconds**, unchanged by delays or retries.

The first server-side RPC or WebSocket access registers the instance. A client `.get(id)` alone does not. The first occurrence is the next matching minute; idle actors are woken through normal actor invocation. Cron methods remain callable RPCs.

**Handler retries default to zero.** `retries: 3` means three additional attempts after confirmed handler failures. Success or exhausted retries advances to the next occurrence. Missed occurrences are processed in order per schedule; separate schedules proceed independently.

**Delivery is always at least once.** Routing failures, executor crashes, lost acknowledgements, and the 15-minute dispatch timeout trigger redelivery without consuming handler retries. Duplicates remain possible with retries disabled. Both kinds of retry back off from one second to five minutes. For external effects, use `(actor ID, method, cron expression, scheduled time)` as an idempotency key.

Schedules, retry counts, and wakeups survive restarts in PostgreSQL (local development: `crons.sqlite3`). Control-plane replicas claim separate jobs with `SKIP LOCKED`, renew leases, and fence stale acknowledgements. Registration creates wakeups transactionally; a separate database-coordinated sweep repairs publication gaps.

Redeployment preserves unchanged schedules and pending occurrences. Retry policy changes apply on the next claim. Added or changed expressions start after publication; removing a decorator cancels pending work, and deleting a deployment removes registrations. Already-dispatched effects cannot be recalled.
