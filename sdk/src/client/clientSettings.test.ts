import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { promisify } from "node:util"

import type { ActorConnection } from "../actor/socket.js"

import { RemoteActorClient } from "./remoteClient.js"
import type { DurableObjectsClientOptions } from "./remoteClient.js"

const options = { token: " token ", namespaceId: "project-1", controlPlaneUrl: "https://CONTROL.example.com:443/" }

test("environment and explicit client settings normalize routes, tokens, and gateway defaults equally", async () => {
    for (const socketGatewayUrl of [undefined, "http://SOCKET.example.com:80/"]) {
        const settings = { ...options, socketGatewayUrl }
        const connections: unknown[] = []
        const dependencies = {
            environment: environmentFor(settings),
            async connectWebSocket(url: string, token: string, metadata: unknown) {
                connections.push({ url, token, metadata })
                return {} as ActorConnection
            }
        }
        await new RemoteActorClient(settings, dependencies).connect("Counter", "one", {})
        await new RemoteActorClient(undefined, dependencies).connect("Counter", "one", {})
        const expected = {
            url: `${socketGatewayUrl ? "ws://socket.example.com" : "wss://control.example.com"}/v1/namespaces/project-1/actors/Counter/one/websocket`,
            token: "token",
            metadata: {}
        }
        assert.deepEqual(connections, [expected, expected])
    }
})

test("environment and explicit client settings report the same validation errors", async () => {
    for (const invalid of [
        { token: " " },
        { namespaceId: "bad/namespace" },
        { controlPlaneUrl: "invalid" },
        { socketGatewayUrl: "https://socket.example.com/path" }
    ]) {
        const settings = { ...options, ...invalid }
        let expected: Error | undefined
        assert.throws(
            () => new RemoteActorClient(settings),
            error => {
                assert.ok(error instanceof Error)
                expected = error
                return true
            }
        )
        assert.ok(expected instanceof Error)
        const client = new RemoteActorClient(undefined, { environment: environmentFor(settings) })
        await assert.rejects(client.connect("Counter", "one", {}), { name: expected.name, message: expected.message })
    }
})

function environmentFor(settings: DurableObjectsClientOptions): NodeJS.ProcessEnv {
    return {
        DURABLE_OBJECT_TOKEN: settings.token,
        DURABLE_OBJECT_NAMESPACE_ID: settings.namespaceId,
        DURABLE_OBJECT_CONTROL_PLANE_URL: settings.controlPlaneUrl,
        DURABLE_OBJECT_SOCKET_GATEWAY_URL: settings.socketGatewayUrl
    }
}

test("clients require explicit credentials even if a discovery file exists", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "actors-no-discovery-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await mkdir(path.join(directory, ".little-actors"))
    await writeFile(
        path.join(directory, ".little-actors/runtime.json"),
        JSON.stringify({
            controlPlaneUrl: "http://localhost:7100",
            apiKey: "stale-key",
            namespaceId: "stale"
        })
    )
    const source = `
        import assert from 'node:assert/strict';
        import { RemoteActorClient } from ${JSON.stringify(new URL("./remoteClient.js", import.meta.url).href)};
        import { SocketProxy } from ${JSON.stringify(new URL("../proxy.js", import.meta.url).href)};
        const client = new RemoteActorClient(undefined, {
            environment: {},
            connectWebSocket: async () => assert.fail('used file credentials')
        });
        await assert.rejects(client.connect('Counter', 'one', {}), /Configure exactly one of apiKey or token/);
        assert.throws(() => new SocketProxy({Room:{}}), /API key/);
    `
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("DURABLE_OBJECT_")))
    await promisify(execFile)(process.execPath, ["--input-type=module", "--eval", source], { cwd: directory, env })
})
