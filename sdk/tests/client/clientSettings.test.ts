import assert from "node:assert/strict"
import { test } from "node:test"

import type { ActorConnection } from "../../src/actor/socket.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import type { DurableActorsClientOptions } from "../../src/client/remoteClient.js"

const options = { projectId: "default", apiKey: " key ", controlPlaneUrl: "https://CONTROL.example.com:443/" }

test("environment and explicit client settings normalize routes and API keys equally", async () => {
    {
        const settings = options
        const connections: unknown[] = []
        const dependencies = {
            environment: environmentFor(settings),
            fetch: async (url: string | URL | Request, init?: RequestInit) => {
                assert.equal(
                    String(url),
                    "https://control.example.com/v1/projects/default/actors/Counter/one/find-websocket"
                )
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
        await new RemoteActorClient(undefined, {
            ...dependencies,
            environment: {
                ...environmentFor({
                    ...settings,
                    projectId: "wrong",
                    apiKey: "wrong",
                    controlPlaneUrl: "https://wrong.example"
                }),
                DURABLE_ACTORS_PROJECT_ID: settings.projectId,
                DURABLE_ACTORS_SECRET: settings.apiKey,
                DURABLE_ACTORS_CONTROL_PLANE_URL: settings.controlPlaneUrl
            }
        }).connect("Counter", "one", {})
        const expected = {
            url: "wss://host.example.com/v1/socket?key=ticket",
            metadata: {}
        }
        assert.deepEqual(connections, [expected, expected, expected])
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

function environmentFor(settings: DurableActorsClientOptions): NodeJS.ProcessEnv {
    return {
        DURABLE_ACTORS_PROJECT_ID: settings.projectId,
        DURABLE_ACTORS_SECRET: settings.apiKey,
        DURABLE_ACTORS_HOME_REGION: settings.homeRegion,
        DURABLE_ACTORS_CONTROL_PLANE_URL: settings.controlPlaneUrl
    }
}

test("backend clients omit authorization without a secret locally and remotely", async () => {
    for (const options of [undefined, { projectId: "private", controlPlaneUrl: "http://192.168.1.1:7100" }]) {
        const client = new RemoteActorClient(options, {
            environment: {},
            fetch: async (url, init) => {
                const origin = options?.controlPlaneUrl ?? "http://127.0.0.1:7100"
                const project = options?.projectId ?? "local"
                assert.equal(String(url), `${origin}/v1/projects/${project}/actors/Counter/one/find-websocket`)
                assert.equal(new Headers(init?.headers).get("authorization"), null)
                return Response.json({ websocketUrl: "ws://127.0.0.1:7100/v1/socket?key=ticket" })
            },
            connectWebSocket: async () => ({}) as ActorConnection
        })
        await client.connect("Counter", "one", {})
    }
})
