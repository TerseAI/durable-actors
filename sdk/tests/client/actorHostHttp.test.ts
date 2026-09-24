import assert from "node:assert/strict"
import { test } from "node:test"

import { HttpActorHostTransport } from "../../src/client-runtime/http.js"
import { ActorProtocolError } from "../../src/errors.js"

const target = { route: "https://host.example", token: "ticket", ownerEpoch: 3, expiresAtMs: 4000000000000 }
const invocation = {
    projectId: "team",
    actorName: "Counter",
    actorId: "one",
    requestId: "request-1",
    method: "increment",
    args: [2]
}

test("HTTP invocation sends the actor ticket, epoch, method and JSON arguments", async () => {
    const methods: string[] = []
    const transport = new HttpActorHostTransport(async (url, init) => {
        assert.equal(url, "https://host.example/v1/projects/team/actors/Counter/one/invoke")
        methods.push(init!.method!)
        if (init?.method === "HEAD") {
            assert.equal(init.body, undefined)
            assert.equal(new Headers(init.headers).get("authorization"), "Bearer ticket")
            assert.ok(init.signal instanceof AbortSignal)
            return new Response(null, { status: 204 })
        }
        assert.equal(init?.method, "POST")
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer ticket")
        assert.equal(init?.redirect, "error")
        assert.deepEqual(JSON.parse(String(init?.body)), {
            requestId: "request-1",
            ownerEpoch: 3,
            method: "increment",
            args: [2]
        })
        return Response.json({ type: "completed", result: 7 })
    })
    assert.deepEqual(await transport.invoke(target, invocation), { type: "completed", result: 7 })
    assert.deepEqual(methods, ["HEAD", "POST"])
})

test("only a pre-dispatch HTTP 401 is an authentication refresh signal", async () => {
    for (const status of [401, 403, 500, 503, 504]) {
        const transport = invocationTransport(async () => new Response("rejected", { status }))
        if (status === 401) assert.deepEqual(await transport.invoke(target, invocation), { type: "unauthenticated" })
        else await assert.rejects(transport.invoke(target, invocation))
    }
    const failure = { type: "failed", code: "unauthenticated", message: "method failed" }
    const transport = invocationTransport(async () => Response.json(failure))
    assert.deepEqual(await transport.invoke(target, invocation), failure)
})

test("HTTP replies require an explicit valid outcome", async () => {
    for (const document of [
        {},
        { type: "completed" },
        { type: "failed", code: 3 },
        { type: "unauthenticated" },
        { type: "not_dispatched" }
    ]) {
        const transport = invocationTransport(async () => Response.json(document))
        await assert.rejects(transport.invoke(target, invocation), ActorProtocolError)
    }
})

test("connection refusals identify requests that were never dispatched", async () => {
    const refused = () => Object.assign(new Error("refused"), { code: "ECONNREFUSED", syscall: "connect" })
    for (const error of [
        refused(),
        new TypeError("fetch failed", { cause: refused() }),
        new TypeError("fetch failed", { cause: new AggregateError([refused(), refused()]) })
    ]) {
        const transport = invocationTransport(async () => {
            throw error
        })
        assert.deepEqual(await transport.invoke(target, invocation), { type: "not_dispatched" })
    }
})

test("unproven transport failures preserve ambiguity", async () => {
    const refused = Object.assign(new Error("refused"), { code: "ECONNREFUSED", syscall: "connect" })
    const cyclic = new Error("cycle")
    cyclic.cause = cyclic
    for (const error of [
        new TypeError("fetch failed"),
        new Error("connect ECONNREFUSED"),
        Object.assign(new Error("refused"), { code: "ECONNREFUSED" }),
        Object.assign(new Error("reset"), { code: "ECONNRESET", syscall: "read", cause: refused }),
        Object.assign(new Error("timeout"), { code: "ETIMEDOUT" }),
        new AggregateError([refused, new Error("connection lost")]),
        Object.assign(new AggregateError([refused, cyclic]), { code: "ECONNREFUSED" }),
        new AggregateError([]),
        cyclic
    ]) {
        const transport = invocationTransport(async () => {
            throw error
        })
        await assert.rejects(transport.invoke(target, invocation), failure => failure === error)
    }
})

test("socket broadcasts use actor-scoped HTTP with the same ownership ticket", async () => {
    const effects = [
        { type: "broadcast", message: { type: "text", data: "hello" }, except_connection_ids: [], tags: [] }
    ]
    const transport = invocationTransport(async (url, init) => {
        assert.equal(url, "https://host.example/v1/projects/team/actors/Counter/one/socket-effects")
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer ticket")
        assert.deepEqual(JSON.parse(String(init?.body)), { ownerEpoch: 3, effects })
        return new Response(null, { status: 204 })
    })
    await transport.publish(target, invocation, effects)
})

test("a failed ping never sends an actor invocation", async () => {
    for (const status of [0, 200, 401, 404, 503]) {
        const methods: string[] = []
        const transport = new HttpActorHostTransport(async (_url, init) => {
            methods.push(init!.method!)
            assert.equal(init?.body, undefined)
            if (status === 0) throw new TypeError("fetch failed", { cause: new Error("other side closed") })
            return new Response(null, { status })
        })
        assert.deepEqual(await transport.invoke(target, invocation), {
            type: status === 401 ? "unauthenticated" : "not_dispatched"
        })
        assert.deepEqual(methods, ["HEAD"])
    }
})

function invocationTransport(fetchRequest: typeof globalThis.fetch): HttpActorHostTransport {
    return new HttpActorHostTransport(async (url, init) =>
        init?.method === "HEAD" ? new Response(null, { status: 204 }) : fetchRequest(url, init)
    )
}
