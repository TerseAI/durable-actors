# One-off alarms

Each actor has one durable deadline, expressed as Unix milliseconds. Setting it replaces the previous deadline; deleting it cancels pending delivery. Changes commit with the invocation's SQLite state. Alarms wake idle actors and survive runtime restarts.

```typescript
import { Actor } from "durable-actors"

class Session extends Actor {
    async start(): Promise<void> { this.setAlarm(Date.now() + 30_000) }
    async cancel(): Promise<void> { this.deleteAlarm() }
    async deadline(): Promise<number | null> { return this.getAlarm() }
    async onAlarm(): Promise<void> { /* check the session timeout */ }
}
```

```python
import time
from durable_actors import Actor

class Session(Actor):
    def start(self) -> None:
        self.set_alarm(time.time_ns() // 1_000_000 + 30_000)
    def cancel(self) -> None:
        self.delete_alarm()
    def deadline(self) -> int | None:
        return self.get_alarm()
    def on_alarm(self) -> None:
        pass  # Check the session timeout.
```

Delivery occurs at or after the deadline; a past deadline runs as soon as possible. A successful handler clears its deadline unless it replaces or cancels it. During the handler, the getter still returns the delivered deadline until changed. Failures and interrupted deliveries retry with exponential backoff; external effects must be idempotent because delivery is at least once. Cancellation cannot interrupt a handler that already started. Actors using `@Reentrant`/`@reentrant` retain their usual shared-state and rollback semantics.

The local runtime stores pending deliveries in its data directory; deployed runtimes use the control plane's PostgreSQL database. Back up that database together with actor storage.
