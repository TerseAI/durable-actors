import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persisted } from "../../src/actor/decorators.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime } from "../../src/host/actor-runtime.js"
import { seed } from "../fixtures/litestream.js"

class Background extends Actor {
    @Persisted count = 0

    async start(fail = false): Promise<number> {
        this.count = 1
        this.waitUntil(async () => {
            this.db.exec("CREATE TABLE IF NOT EXISTS jobs(value INTEGER)")
            this.db.exec("INSERT INTO jobs VALUES (2)")
            this.count = 2
            if (fail) {
                Reflect.set(this, "undeclared", true)
                throw new Error("background failed")
            }
            this.broadcast("finished")
            return new Map([["ignored", 1n]])
        })
        if (fail)
            this.waitUntil(async () => {
                this.count += 10
            })
        return this.count
    }

    async read(): Promise<number> {
        return this.count
    }
}

test("background work has an independent invocation and rollback", async () => {
    const runtime = new ActorRuntime(
        registerActorClass(Background, {
            actorName: "Background",
            fields: [{ name: "count", persistence: Persistence.Persisted }]
        }),
        () => {}
    )
    const command = {
        type: "invoke" as const,
        request_id: "background-test",
        actor: { project_id: "local", actor_name: "Background", actor_id: "one" },
        method: "start",
        args: [true],
        sqlite: await seed()
    }
    try {
        const accepted = await runtime.handle(command)
        assert.equal(accepted.type, "invoked")
        if (accepted.type !== "invoked") return
        assert.equal(accepted.result, 1)
        assert.equal(accepted.background_tasks?.length, 2)
        const failed = await runtime.handle({
            ...command,
            method: "__background",
            args: [accepted.background_tasks![0]]
        })
        assert.equal(failed.type, "failed")
        const read = await runtime.handle({ ...command, method: "read", args: [] })
        assert.equal(read.type === "invoked" && read.result, 1)
        const sibling = await runtime.handle({
            ...command,
            method: "__background",
            args: [accepted.background_tasks![1]]
        })
        assert.equal(sibling.type, "invoked")
        const siblingState = await runtime.handle({ ...command, method: "read", args: [] })
        assert.equal(siblingState.type === "invoked" && siblingState.result, 11)
        const next = await runtime.handle({ ...command, args: [false] })
        assert.equal(next.type, "invoked")
        if (next.type !== "invoked") return
        const finished = await runtime.handle({ ...command, method: "__background", args: [...next.background_tasks!] })
        assert.equal(finished.type, "invoked")
        const state = await runtime.handle({ ...command, method: "read", args: [] })
        assert.equal(state.type === "invoked" && state.result, 2)
    } finally {
        runtime.close()
    }
})
