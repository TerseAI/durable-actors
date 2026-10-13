import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { ActorRuntime, SqliteState } from "../fixtures/actor-runtime.js"
import { seed } from "../fixtures/litestream.js"
import { assertReply } from "../fixtures/reply.js"

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

    async failAfterSet(): Promise<void> {
        this.alarm.set(3_000)
        throw new Error("test failure")
    }
}

function actorInvoker(runtime: ActorRuntime, sqlite: SqliteState) {
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
            args,
            sqlite
        })
}

test("actors can read, replace, and delete their alarm", async context => {
    const definition = registerActorClass(AlarmProbe, {
        actorName: "AlarmProbe",
        fields: []
    })

    const runtime = new ActorRuntime(definition, () => {})
    context.after(() => runtime.close())
    const sqlite = await seed()
    const invoke = actorInvoker(runtime, sqlite)
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

    // test non-atomic method, error sets the alarm anyway
    assertReply(await invoke("setAlarm", [2_000]), {
        type: "invoked",
        result: 2_000
    })
    assertReply(await invoke("failAfterSet"), {
        type: "failed",
        code: "actor_method_failed",
        message: "test failure"
    })
    assertReply(await invoke("readAlarm"), {
        type: "invoked",
        result: 3_000
    })
})

test("failed alarm set in atomic does rollback and is not saved", async context => {
    const definition = registerActorClass(AlarmProbe, {
        actorName: "AlarmProbe",
        atomicMethods: ["failAfterSet"],
        fields: []
    })
    const runtime = new ActorRuntime(definition, () => {})

    context.after(() => runtime.close())
    const sqlite = await seed()
    const invoke = actorInvoker(runtime, sqlite)
    assertReply(await invoke("setAlarm", [2_000]), {
        type: "invoked",
        result: 2_000
    })
    assertReply(await invoke("failAfterSet"), {
        type: "failed",
        code: "actor_method_failed",
        message: "test failure"
    })
    assertReply(await invoke("readAlarm"), {
        type: "invoked",
        result: 2_000
    })
})
