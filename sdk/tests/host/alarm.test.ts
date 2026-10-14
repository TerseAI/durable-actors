import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import type { ActorAlarm } from "../../src/index.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

let escapedAlarm: ActorAlarm

class AlarmProbe extends Actor {
    async readAlarm(): Promise<number | null> {
        return this.alarm.get()
    }
    async setAlarm(timestampMs: number): Promise<number | null> {
        this.alarm.set(timestampMs)
        return this.alarm.get()
    }
    async deleteAlarm(): Promise<number | null> {
        this.alarm.delete()
        return this.alarm.get()
    }

    async failAfterImmediateSet(): Promise<void> {
        this.alarm.set(3_000)
        throw new Error("test failure")
    }

    async failAfterAtomicSet(): Promise<void> {
        this.alarm.set(3_000)
        throw new Error("test failure")
    }

    async escapeAlarm(): Promise<void> {
        escapedAlarm = this.alarm
    }
}

const definition = registerActorClass(AlarmProbe, {
    actorName: "AlarmProbe",
    atomicMethods: ["failAfterAtomicSet"],
    fields: []
})

function actorInvoker(runtime: ActorRuntime) {
    return (method: string, args: number[] = []) =>
        runtime.handle({
            type: "invoke",
            request_id: method,
            actor: {
                project_id: "test",
                actor_name: "AlarmProbe",
                actor_id: "one"
            },
            method,
            args
        })
}

test("actors can read, replace, and delete their alarm", async context => {
    const { runtime, host } = createRuntime()
    context.after(async () => {
        await runtime.close()
        host.close()
    })
    const invoke = actorInvoker(runtime)
    assertReply(await invoke("readAlarm"), {
        type: "invoked",
        result: null
    })
    assertReply(await invoke("setAlarm", [1_000]), {
        type: "invoked",
        result: 1_000
    })
    assertReply(await invoke("setAlarm", [2_000]), {
        type: "invoked",
        result: 2_000
    })
    assertReply(await invoke("deleteAlarm"), {
        type: "invoked",
        result: null
    })

    assertReply(await invoke("setAlarm", [2_000]), {
        type: "invoked",
        result: 2_000
    })
    assertReply(await invoke("failAfterImmediateSet"), {
        type: "failed",
        code: "actor_method_failed",
        message: "test failure"
    })
    assertReply(await invoke("readAlarm"), {
        type: "invoked",
        result: 3_000
    })

    assertReply(await invoke("setAlarm", [2_000]), {
        type: "invoked",
        result: 2_000
    })
    assertReply(await invoke("failAfterAtomicSet"), {
        type: "failed",
        code: "actor_method_failed",
        message: "test failure"
    })
    assertReply(await invoke("readAlarm"), {
        type: "invoked",
        result: 2_000
    })
})

test("alarm timestamps must be safe integers", async context => {
    const { runtime, host } = createRuntime()
    context.after(async () => {
        await runtime.close()
        host.close()
    })
    const invoke = actorInvoker(runtime)
    for (const timestamp of [NaN, Infinity, -Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
        assertReply(await invoke("setAlarm", [timestamp]), {
            type: "failed",
            code: "actor_method_failed",
            message: "actor alarm timestamp must be a safe integer"
        })
    }
})

test("alarm handles expire after their invocation", async context => {
    const { runtime, host } = createRuntime()
    context.after(async () => {
        await runtime.close()
        host.close()
    })
    const invoke = actorInvoker(runtime)
    assertReply(await invoke("escapeAlarm"), { type: "invoked", result: null })
    assert.throws(() => escapedAlarm.get(), /outside its invocation/)
    assert.throws(() => escapedAlarm.set(1_000), /outside its invocation/)
    assert.throws(() => escapedAlarm.delete(), /outside its invocation/)
})

function createRuntime() {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(definition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    return { runtime, host }
}
