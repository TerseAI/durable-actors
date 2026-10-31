import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persisted } from "../../src/actor/decorators.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

class LifecycleProbe extends Actor {
    @Persisted calls = 0
    @Persisted asleep = false

    async onSleep(): Promise<void> {
        this.asleep = true
        this.db.exec("CREATE TABLE IF NOT EXISTS sleeps (calls INTEGER)")
        this.db.exec("INSERT INTO sleeps (calls) VALUES (?)", this.calls)
    }

    async snapshot(): Promise<{ calls: number; asleep: boolean }> {
        this.calls += 1
        return { calls: this.calls, asleep: this.asleep }
    }

    async slow(): Promise<number> {
        await new Promise(resolve => setTimeout(resolve, 50))
        this.calls += 1
        return this.calls
    }
}

const probeIdentity = { project_id: "test", actor_name: "LifecycleProbe", actor_id: "one" }
const probeDefinition = registerActorClass(LifecycleProbe, {
    actorName: "LifecycleProbe",
    fields: [
        { name: "calls", persistence: Persistence.Persisted },
        { name: "asleep", persistence: Persistence.Persisted }
    ]
})

function invoke(runtime: ActorRuntime, method: string) {
    return runtime.handle({ type: "invoke", request_id: method, actor: probeIdentity, method, args: [] })
}

test("onSleep persists fields and SQL on graceful shutdown before the instance is discarded", async () => {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(probeDefinition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    assertReply(await invoke(runtime, "snapshot"), { type: "invoked", result: { calls: 1, asleep: false } })
    await runtime.close(true)
    const reopened = host.reopen()
    const restored = new ActorRuntime(probeDefinition, () => {}, undefined, undefined, new DatabaseProxy(reopened.call))
    try {
        assertReply(await invoke(restored, "snapshot"), { type: "invoked", result: { calls: 2, asleep: true } })
        assert.equal(Number(reopened.query("SELECT calls FROM sleeps")[0]?.calls), 1)
    } finally {
        await restored.close(false)
        reopened.close()
    }
})

test("a non-graceful close skips onSleep", async () => {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(probeDefinition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    assertReply(await invoke(runtime, "snapshot"), { type: "invoked", result: { calls: 1, asleep: false } })
    await runtime.close(false)
    assert.equal(host.fields().asleep, false)
    host.close()
})
