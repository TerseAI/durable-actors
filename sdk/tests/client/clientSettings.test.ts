import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { promisify } from "node:util"

import type { ActorConnection } from "../../src/actor/socket.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import type { DurableObjectsClientOptions } from "../../src/client/remoteClient.js"

const options = { projectId: "default", apiKey: " key ", controlPlaneUrl: "https://CONTROL.example.com:443/" }

test("environment and explicit client settings normalize routes and API keys equally", async () => {
    {
        const settings = options
        const connections: unknown[] = []
        const dependencies = {
            environment: environmentFor(settings),
            fetch: async (url: string | URL | Request, init?: RequestInit) => {
                assert.equal(String(url), "https://control.example.com/v1/projects/default/actors/Counter/one/connect")
                assert.equal(new Headers(init?.headers).get("authorization"), "Bearer key")
                return Response.json({ websocketUrl: "wss://host.example.com/v1/socket?key=ticket", key: "ticket" })
            },
            async connectWebSocket(url: string, metadata: unknown) {
                connections.push({ url, metadata })
                return {} as ActorConnection
            }
        }
        await new RemoteActorClient(settings, dependencies).connect("Counter", "one", {})
        await new RemoteActorClient(undefined, dependencies).connect("Counter", "one", {})
        const expected = {
            url: "wss://host.example.com/v1/socket?key=ticket",
            metadata: {}
        }
        assert.deepEqual(connections, [expected, expected])
    }
})

test("environment and explicit client settings report the same validation errors", async () => {
    for (const invalid of [
        { projectId: "default", apiKey: " " },
        { homeRegion: "bad/region" },
        { projectId: "default", controlPlaneUrl: "invalid" }
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
        DURABLE_OBJECT_PROJECT_ID: settings.projectId,
        DURABLE_OBJECT_API_KEY: settings.apiKey,
        DURABLE_OBJECT_HOME_REGION: settings.homeRegion,
        DURABLE_OBJECT_CONTROL_PLANE_URL: settings.controlPlaneUrl
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
            apiKey: "stale-key"
        })
    )
    const source = `
        import assert from 'node:assert/strict';
        import { RemoteActorClient } from ${JSON.stringify(new URL("../../src/client/remoteClient.js", import.meta.url).href)};
        import { SocketProxy } from ${JSON.stringify(new URL("../../src/proxy.js", import.meta.url).href)};
        const client = new RemoteActorClient(undefined, {
            environment: {},
            connectWebSocket: async () => assert.fail('used file credentials')
        });
        await assert.rejects(client.connect('Counter', 'one', {}), /client settings are invalid/);
        assert.throws(() => new SocketProxy({Room:{}}, {projectId:"default"}), /API key/);
    `
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("DURABLE_OBJECT_")))
    await promisify(execFile)(process.execPath, ["--input-type=module", "--eval", source], { cwd: directory, env })
})
