# @durable-actors/pi-durable

Run a Pi Durable agent inside a Durable Actor. The actor owns the database; the adapter owns the Pi harness and conversation.

```ts
import { DurablePiAgent } from "@durable-actors/pi-durable"
import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry } from "@earendil-works/pi-durable"
import { Actor } from "durable-actors"

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()

export class PiAgent extends Actor {
    private agent = new DurablePiAgent(this, {
        models,
        registry,
        model: { provider: "openai", modelId: "gpt-5-mini" }
    })

    async prompt(text: string): Promise<string> {
        const result = await this.agent.prompt(text)
        return result.text
    }
}
```

`models`, `registry`, and `model` are required. Install your extensions and tools in the registry before prompting. The private `DurablePiAgent` field is an `ActorResource`, so the actor compiler treats it as ephemeral. Pi's conversation is persisted separately in the actor's SQLite database.

Construction does not access storage. Each `prompt()` opens the harness, submits input, waits for a completed answer, returns `{ text }`, and closes the harness. Failed prompts reject with Pi's reason and error detail. A subsequent prompt restores the same root conversation and resumes pending work.

## Native Pi access

`agent.session` exposes `PiSession`. Use `run()` inside an actor method to work directly with Pi's native `harness`, root `conversation`, and Chord `context`:

```ts
async reset(): Promise<void> {
    await this.agent.session.run(({ conversation, context }) =>
        conversation.reset(undefined, context)
    )
}

async usage() {
    return this.agent.session.run(({ harness, context }) =>
        harness.usage(context)
    )
}
```

The callback can use Pi's APIs for configuration, documents, tools, watches, additional conversations, and tasks directly. Await its work within the callback: the harness closes when the callback finishes, including when it throws. Keep native handles and watches inside that scope. `prompt()` uses the same lifecycle internally.

Adapter options extend Pi's native `HarnessOptions`, adding the required `model`. Pass `env`, `settings`, `conversationCreated`, and `onReport` directly. Register integration tools, including Pi-compatible Composio tools, in the supplied Pi registry; they use Pi's normal tool execution without an adapter protocol. Built-in filesystem tools can use an execution environment supplied through `env`.

Use one agent per actor and ordinary serialized actor methods. Run session callbacks sequentially. This initial adapter replays an append-only commit log into Pi's `MemoryStorage` on each operation, so memory and reopen time grow with history. It does not schedule background wakeups. Native watches and events are available inside `run()`; the `prompt()` convenience returns completed text.

See the [actor and CLI example](../../examples/pi/README.md). This package is under development in this workspace and has not been published.
