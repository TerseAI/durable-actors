import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { ActorRuntime } from "../../src/host/actor-runtime.js"
import { seed } from "../fixtures/litestream.js"

test("alarm generations fence replacement, cancellation and duplicate delivery", async () => {
    class Timer extends Actor {
        async schedule(deadline: number) {
            this.setAlarm(deadline)
            return this.getAlarm()
        }
        async current() {
            return this.getAlarm()
        }
        async cancel() {
            this.deleteAlarm()
        }
        async onAlarm() {
            this.db.exec("CREATE TABLE IF NOT EXISTS fired (value INTEGER)")
            this.db.exec("INSERT INTO fired VALUES (1)")
        }
        async count() {
            return this.db.exec<{ count: number }>("SELECT count(*) AS count FROM fired")[0]!.count
        }
    }
    const definition = registerActorClass(Timer, { actorName: "Timer", fields: [] })
    assert.equal(definition.methods.has("onAlarm"), false)
    const runtime = new ActorRuntime(definition, () => {})
    const actor = { project_id: "local", actor_name: "Timer", actor_id: "one" }
    const sqlite = await seed(null)
    const call = (method: string, args: any[] = []) =>
        runtime.handle({ type: "invoke", actor, request_id: "test", method, args, sqlite })
    try {
        const first = await call("schedule", [1])
        assert.equal(first.type, "invoked")
        if (first.type !== "invoked") return
        const second = await call("schedule", [2])
        assert.equal(second.type, "invoked")
        if (second.type !== "invoked") return
        assert.notEqual(first.sqlite.alarm!.generation, second.sqlite.alarm!.generation)
        await call("__alarm", [first.sqlite.alarm!.generation])
        await call("__alarm", [second.sqlite.alarm!.generation])
        await call("__alarm", [second.sqlite.alarm!.generation])
        assert.deepEqual(((await call("count")) as { result: unknown }).result, 1)
        const cancelled = await call("schedule", [3])
        assert.equal(cancelled.type, "invoked")
        if (cancelled.type !== "invoked") return
        await call("cancel")
        await call("__alarm", [cancelled.sqlite.alarm!.generation])
        assert.deepEqual(((await call("count")) as { result: unknown }).result, 1)
        const retained = await call("schedule", [4])
        assert.equal(retained.type, "invoked")
        if (retained.type !== "invoked") return
        Reflect.deleteProperty(Timer.prototype, "onAlarm")
        assert.equal((await call("__alarm", [retained.sqlite.alarm!.generation])).type, "failed")
        assert.equal(((await call("current")) as { result: unknown }).result, 4)
    } finally {
        runtime.close()
    }
})
