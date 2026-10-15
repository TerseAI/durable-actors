import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persisted } from "../../src/actor/decorators.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

class LifecycleProbe extends Actor {
    @Persisted wakes = 0
    @Persisted asleep = false

    async onWake(): Promise<void> {
        this.wakes += 1
        this.db.exec("CREATE TABLE IF NOT EXISTS wakes (n INTEGER)")
        this.db.exec("INSERT INTO wakes (n) VALUES (?)", this.wakes)
    }

    async onSleep(): Promise<void> {
        this.asleep = true
        this.db.exec("CREATE TABLE IF NOT EXISTS sleeps (n INTEGER)")
        this.db.exec("INSERT INTO sleeps (n) VALUES (1)")
    }

    async snapshot(): Promise<{ wakes: number; asleep: boolean; wakeRows: number }> {
        return {
            wakes: this.wakes,
            asleep: this.asleep,
            wakeRows: this.db.exec<{ n: number }>("SELECT count(*) AS n FROM wakes")[0]?.n ?? 0
        }
    }

    async fail(): Promise<never> {
        throw new Error("failed")
    }
}

class ThrowingWake extends Actor {
    async onWake(): Promise<void> {
        throw new Error("wake failed")
    }

    async ping(): Promise<string> {
        return "ok"
    }
}

const probeIdentity = { project_id: "test", actor_name: "LifecycleProbe", actor_id: "one" }
const probeDefinition = registerActorClass(LifecycleProbe, {
    actorName: "LifecycleProbe",
    fields: [
        { name: "wakes", persistence: Persistence.Persisted },
        { name: "asleep", persistence: Persistence.Persisted }
    ]
})
const throwingDefinition = registerActorClass(ThrowingWake, { actorName: "ThrowingWake", fields: [] })

function invoke(runtime: ActorRuntime, method: string, actor = probeIdentity) {
    return runtime.handle({
        type: "invoke",
        request_id: method,
        actor,
        method,
        args: []
    })
}

test("onWake runs once per activation, persists fields and SQL, and is skipped after failed calls", async context => {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(probeDefinition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    context.after(async () => {
        await runtime.close()
        host.close()
    })
    assertReply(await invoke(runtime, "snapshot"), {
        type: "invoked",
        result: { wakes: 1, asleep: false, wakeRows: 1 }
    })
    assertReply(await invoke(runtime, "snapshot"), {
        type: "invoked",
        result: { wakes: 1, asleep: false, wakeRows: 1 }
    })
    assert.equal((await invoke(runtime, "fail")).type, "failed")
    assertReply(await invoke(runtime, "snapshot"), {
        type: "invoked",
        result: { wakes: 1, asleep: false, wakeRows: 1 }
    })
    assert.equal(host.fields().wakes, 1)
})

test("hydrate runs onWake before returning", async context => {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(probeDefinition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    context.after(async () => {
        await runtime.close()
        host.close()
    })
    assertReply(await runtime.handle({ type: "hydrate", actor: probeIdentity }), { type: "hydrated" })
    assert.equal(host.fields().wakes, 1)
})

test("onSleep persists fields and SQL before the instance is discarded", async () => {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(probeDefinition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    assertReply(await invoke(runtime, "snapshot"), {
        type: "invoked",
        result: { wakes: 1, asleep: false, wakeRows: 1 }
    })
    await runtime.close()
    const reopened = host.reopen()
    const restored = new ActorRuntime(probeDefinition, () => {}, undefined, undefined, new DatabaseProxy(reopened.call))
    try {
        assertReply(await invoke(restored, "snapshot"), {
            type: "invoked",
            result: { wakes: 2, asleep: true, wakeRows: 2 }
        })
        assert.equal(Number(reopened.query("SELECT count(*) AS n FROM sleeps")[0]?.n), 1)
    } finally {
        await restored.close()
        reopened.close()
    }
})

test("a throwing onWake fails activation without running actor methods", async context => {
    const runtime = new ActorRuntime(throwingDefinition, () => {})
    context.after(() => runtime.close())
    const reply = await invoke(runtime, "ping", {
        project_id: "test",
        actor_name: "ThrowingWake",
        actor_id: "one"
    })
    assert.equal(reply.type, "failed")
    assert.equal(reply.type === "failed" ? reply.code : undefined, "invalid_actor_state")
    assert.match(reply.type === "failed" ? reply.message : "", /wake failed/u)
})
