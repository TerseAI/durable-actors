import assert from "node:assert/strict"
import { test } from "node:test"
import { z } from "zod"

import { Actor } from "../../src/actor/actor.js"
import { ActorValidationError } from "../../src/errors.js"
import { runWithActorClientForTests } from "../fixtures/actorClient.js"

class ChatRoom extends Actor {
    async history(): Promise<readonly string[]> {
        return []
    }
}

test("actor references broadcast without invoking a customer actor method", async () => {
    const broadcasts: unknown[] = []
    await runWithActorClientForTests(
        {
            invoke: async () => assert.fail("broadcast invoked a customer actor method"),
            broadcast: async request => {
                broadcasts.push(request)
            },
            requestId: () => "request-1"
        },
        () => ChatRoom.get("room-1").broadcast({ text: "hello" })
    )
    assert.deepEqual(broadcasts, [
        {
            requestId: "request-1",
            actorName: "ChatRoom",
            actorId: "room-1",
            message: { type: "text", data: JSON.stringify({ text: "hello" }) }
        }
    ])
})

test("actor references enforce declared metadata and broadcast schemas before dispatch", async () => {
    class SchemaReferenceRoom extends Actor<{ userId: string }, { text: string }> {
        static schemas = { metadata: z.object({ userId: z.string() }), outgoing: z.object({ text: z.string() }) }
    }
    await runWithActorClientForTests(
        {
            invoke: async () => assert.fail("unexpected actor invocation"),
            connect: async () => assert.fail("invalid metadata was dispatched"),
            broadcast: async () => assert.fail("invalid broadcast was dispatched")
        },
        async () => {
            const room = SchemaReferenceRoom.get("one")
            assert.throws(() => room.connect({ userId: 123 } as never), ActorValidationError)
            assert.throws(() => room.broadcast({ text: 123 } as never), ActorValidationError)
        }
    )
})
