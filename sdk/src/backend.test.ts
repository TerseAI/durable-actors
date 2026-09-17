import assert from "node:assert/strict"
import { test } from "node:test"

import { createActorStub } from "./backend.js"
import { runWithActorClient } from "./client/client.js"

interface RoomStub {
    send(text: string, ...tags: string[]): Promise<string>
    clear(): Promise<void>
    empty(): Promise<null>
}

const methods = [
    { name: "send", result: "value" },
    { name: "clear", result: "void" },
    { name: "empty", result: "value" }
] as const

test("backend stubs forward only declared methods and preserve value, void and error results", async () => {
    const calls: unknown[][] = []
    const failure = new Error("actor failed")
    const stub = createActorStub<RoomStub>("Room", "one", methods, {
        async invoke(...args) {
            calls.push(args)
            if (args[3][0] === "fail") throw failure
            return args[2] === "send" ? "sent" : null
        }
    })
    assert.equal(await stub.send("hello", "tag"), "sent")
    assert.equal(await stub.clear(), undefined)
    assert.equal(await stub.empty(), null)
    assert.deepEqual(calls, [
        ["Room", "one", "send", ["hello", "tag"]],
        ["Room", "one", "clear", []],
        ["Room", "one", "empty", []]
    ])
    await assert.rejects(stub.send("fail"), error => error === failure)
    assert.equal(await Promise.resolve(stub), stub)
    assert.equal(Object.getPrototypeOf(stub), null)
    assert.deepEqual(Object.keys(stub), ["send", "clear", "empty"])
})

test("backend stubs use the existing client lazily within the current invocation scope", async () => {
    const stub = createActorStub<RoomStub>("Room", "one", methods)
    const value = await runWithActorClient(
        {
            async invoke() {
                return "scoped"
            },
            async connect() {
                throw new Error("unused")
            },
            async broadcast() {}
        },
        () => stub.send("hello")
    )
    assert.equal(value, "scoped")
})

test("backend stubs reject invalid identities and unsafe method names before dispatch", () => {
    assert.throws(() => createActorStub("Room", "bad/id", []), /actor ID/)
    assert.throws(() => createActorStub("bad/type", "one", []), /actor type/)
    assert.throws(() => createActorStub("Room", "one", [{ name: "then", result: "value" }]), /reserved/)
})

test("backend stubs trim omitted trailing arguments and reject undefined holes before JSON transport", async () => {
    const received: (readonly unknown[])[] = []
    const stub = createActorStub<{ send(text?: string, count?: number): Promise<void> }>(
        "Room",
        "one",
        [{ name: "send", result: "void" }],
        {
            async invoke(_type, _id, _method, args) {
                received.push(args)
            }
        }
    )
    await stub.send("hello", undefined)
    await stub.send(undefined)
    assert.deepEqual(received, [["hello"], []])
    await assert.rejects(stub.send(undefined, 2), /undefined.*argument/)
    assert.equal(received.length, 2)
})
