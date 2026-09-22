import { build } from "esbuild"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"
import ts from "typescript"

import { ActorCompiler } from "../../../src/compiler/actor-compiler.js"

test("generates an actor-specific proxy from backend metadata types", async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-proxy-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await mkdir(path.join(directory, "node_modules"))
    await symlink(
        fileURLToPath(new URL("../../../../", import.meta.url)),
        path.join(directory, "node_modules/durable-actors")
    )
    await writeFile(path.join(directory, "package.json"), JSON.stringify({ type: "module" }))
    const entrypoint = path.join(directory, "actors.ts")
    await writeFile(
        entrypoint,
        `import { Actor } from "durable-actors"
        interface Member { userId: string; profile?: { displayName: string } }
        export class Room extends Actor<Member, { type: "post"; text: string }, never> {}
        export class Counter extends Actor<{ tenantId: number; role: "viewer" | "editor" }, number, never> {}
        throw new Error("generation must not execute backend code")`
    )
    const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
    await generateClient(
        new ActorCompiler().compile(entrypoint).map(actor => actor.contract),
        directory
    )
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `import { ActorProxy } from "./index.js"
        import type { ActorAuthorization } from "./index.js"
        import { actors } from "./index.js"
        const grant: Promise<{ websocketUrl: string; homeRegion: string; connectByMs: number; authorizedUntilMs: number }> = actors.Room.prepareWebsocket({ actorId: "lobby", metadata: { userId: "alice" } })
        actors.Counter.prepareWebsocket({ actorId: "one", metadata: { tenantId: 1, role: "viewer" }, authorizationLifetimeMs: 60000 })
        // @ts-expect-error unknown actor
        actors.Missing.prepareWebsocket({ actorId: "one", metadata: {} })
        // @ts-expect-error actor name is selected by the helper
        actors.Room.prepareWebsocket({ actorName: "Counter", actorId: "one", metadata: { userId: "alice" } })
        // @ts-expect-error metadata belongs to another actor
        actors.Room.prepareWebsocket({ actorId: "one", metadata: { tenantId: 1, role: "viewer" } })
        // @ts-expect-error required metadata is missing
        actors.Room.prepareWebsocket({ actorId: "one" })
        // @ts-expect-error nested metadata is typed
        actors.Room.prepareWebsocket({ actorId: "one", metadata: { userId: "alice", profile: { displayName: 1 } } })
        // @ts-expect-error metadata literals are preserved
        actors.Counter.prepareWebsocket({ actorId: "one", metadata: { tenantId: 1, role: "admin" } })
        ActorProxy.handle({ actorName: "Room", actorId: "lobby", metadata: { userId: "alice" } })
        ActorProxy.handle({ actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "viewer" } })
        const proxy = new ActorProxy({ controlPlaneUrl: "https://actors.example.com", apiKey: "secret" })
        proxy.handle({ actorName: "Room", actorId: "lobby", metadata: { userId: "alice", profile: { displayName: "Alice" } } })
        // @ts-expect-error unknown actor
        ActorProxy.handle({ actorName: "Missing", actorId: "one", metadata: {} })
        // @ts-expect-error metadata belongs to another actor
        ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: { tenantId: 1, role: "viewer" } })
        // @ts-expect-error required metadata is missing
        ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: {} })
        // @ts-expect-error nested metadata is typed
        ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: { userId: "alice", profile: { displayName: 1 } } })
        // @ts-expect-error metadata literals are preserved
        proxy.handle({ actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "admin" } })
        function authorize(value: ActorAuthorization) {
            if (value.actorName === "Room") value.metadata.userId.toUpperCase()
            else value.metadata.tenantId.toFixed()
        }
`
    )
    checkTypes(consumer)
    const proxyFile = path.join(directory, "proxy.mjs")
    await build({
        entryPoints: [path.join(directory, "index.ts")],
        bundle: true,
        platform: "node",
        format: "esm",
        external: ["durable-actors/generated"],
        outfile: proxyFile,
        logLevel: "silent"
    })
    const { actors, ActorProxy } = await import(pathToFileURL(proxyFile).href)
    const requests: { url: string; metadata: unknown }[] = []
    const fetch = async (url: unknown, init?: RequestInit) => {
        requests.push({ url: String(url), metadata: JSON.parse(init!.body as string).metadata })
        return Response.json({
            websocketUrl: "wss://actors.example.com/v1/socket?key=ticket",
            homeRegion: "north-america-east",
            connectByMs: 1000,
            authorizedUntilMs: 900000
        })
    }
    const options = { projectId: "default", controlPlaneUrl: "https://actors.example.com", apiKey: "secret" }
    const proxy = new ActorProxy(options, { fetch })
    for (const authorization of [
        { actorName: "Room", actorId: "one", metadata: { userId: "alice" } },
        { actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "editor" } }
    ])
        assert.equal((await proxy.handle(authorization)).websocketUrl, "wss://actors.example.com/v1/socket?key=ticket")
    assert.deepEqual(
        requests.map(request => request.metadata),
        [{ userId: "alice" }, { tenantId: 1, role: "editor" }]
    )
    assert.match(requests[1]!.url, /actors\/Counter\/one\/find-websocket$/)
    for (const authorization of [
        { actorName: "Room", actorId: "one", metadata: {} },
        { actorName: "Room", actorId: "one", metadata: { userId: 1 } },
        { actorName: "Room", actorId: "one", metadata: { userId: "alice", profile: { displayName: 1 } } },
        { actorName: "Counter", actorId: "one", metadata: { tenantId: 1, role: "admin" } }
    ])
        assert.equal((await proxy.handle(authorization)).websocketUrl, "wss://actors.example.com/v1/socket?key=ticket")
    for (const authorization of [
        { actorName: "Missing", actorId: "one", metadata: {} },
        { actorName: "toString", actorId: "one", metadata: {} }
    ])
        await assert.rejects(proxy.handle(authorization), /metadata|actor name/i)
    assert.equal(requests.length, 6, "unknown actors must fail before issuing a ticket")
    const originalFetch = globalThis.fetch
    globalThis.fetch = fetch
    t.after(() => {
        globalThis.fetch = originalFetch
    })
    const original = {
        project: process.env.DURABLE_ACTORS_PROJECT_ID,
        url: process.env.DURABLE_ACTORS_CONTROL_PLANE_URL,
        key: process.env.DURABLE_ACTORS_API_KEY
    }
    t.after(() => {
        for (const [key, value] of Object.entries({
            DURABLE_ACTORS_PROJECT_ID: original.project,
            DURABLE_ACTORS_CONTROL_PLANE_URL: original.url,
            DURABLE_ACTORS_API_KEY: original.key
        })) {
            if (value === undefined) delete process.env[key]
            else process.env[key] = value
        }
    })
    process.env.DURABLE_ACTORS_PROJECT_ID = options.projectId
    process.env.DURABLE_ACTORS_CONTROL_PLANE_URL = options.controlPlaneUrl
    process.env.DURABLE_ACTORS_API_KEY = options.apiKey
    assert.deepEqual(await actors.Room.prepareWebsocket({ actorId: "lobby", metadata: { userId: "alice" } }), {
        websocketUrl: "wss://actors.example.com/v1/socket?key=ticket",
        homeRegion: "north-america-east",
        connectByMs: 1000,
        authorizedUntilMs: 900000
    })
    assert.equal(
        requests.at(-1)!.url,
        "https://actors.example.com/v1/projects/default/actors/Room/lobby/find-websocket"
    )
    const issued: { url: string; headers: Headers; body: unknown }[] = []
    await actors.Counter.prepareWebsocket(
        { actorId: "one", metadata: { tenantId: 1, role: "viewer" }, authorizationLifetimeMs: 60000 },
        { ...options },
        {
            fetch: async (url: unknown, init: RequestInit) => {
                issued.push({
                    url: String(url),
                    headers: new Headers(init.headers),
                    body: JSON.parse(init.body as string)
                })
                return Response.json({
                    websocketUrl: "wss://actors.example.com/v1/socket?key=ticket",
                    homeRegion: "north-america-east",
                    connectByMs: 1000,
                    authorizedUntilMs: 900000
                })
            }
        }
    )
    assert.equal(issued[0]!.url, "https://actors.example.com/v1/projects/default/actors/Counter/one/find-websocket")
    assert.equal(issued[0]!.headers.get("authorization"), "Bearer secret")
    assert.deepEqual(issued[0]!.body, {
        metadata: { tenantId: 1, role: "viewer" },
        authorizationLifetimeMs: 60000
    })
    await assert.rejects(
        actors.Room.prepareWebsocket({ actorId: "lobby", metadata: { userId: "alice" } }, options, {
            fetch: async () => new Response(null, { status: 403 })
        }),
        /HTTP 403/
    )
    assert.equal(
        (await ActorProxy.handle({ actorName: "Room", actorId: "one", metadata: { userId: "alice" } })).websocketUrl,
        "wss://actors.example.com/v1/socket?key=ticket"
    )
})

test("generates backend contracts for actors without outgoing application messages", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-never-"))
    try {
        const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
        await generateClient(
            [
                {
                    version: 1,
                    actorName: "Counter",
                    emittable: [],
                    schema: {
                        definitions: {
                            Metadata: { type: "object" },
                            Incoming: { type: "string" },
                            Outgoing: false,
                            State: { type: "object" }
                        }
                    }
                }
            ],
            directory
        )
        assert.match(await readFile(path.join(directory, "index.ts"), "utf8"), /export type Outgoing = never/)
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test("actor names cannot collide with generated entrypoint or helper bindings", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-names-"))
    try {
        const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
        const names = [
            "actors",
            "clients",
            "$createClient",
            "frontend",
            "proxy",
            "ActorClient",
            "ActorProxy",
            "SocketProxy",
            "ActorAuthorization",
            "createClient",
            "createBrowserClient",
            "validators",
            "ActorDescriptor",
            "Connection",
            "Authorization",
            "Metadata",
            "Incoming",
            "Outgoing",
            "State"
        ]
        await generateClient(
            names.map(actorName => ({
                version: 1,
                actorName,
                emittable: [],
                schema: {
                    definitions: {
                        Metadata: { type: "object" },
                        Incoming: { type: "string" },
                        Outgoing: { type: "string" },
                        State: { type: "object" }
                    }
                }
            })),
            directory
        )
        checkTypes(path.join(directory, "index.ts"))
        await build({
            entryPoints: [path.join(directory, "index.ts")],
            bundle: true,
            platform: "browser",
            format: "esm",
            write: false,
            alias: {
                "durable-actors/generated": fileURLToPath(
                    new URL("../../../../src/generated.browser.ts", import.meta.url)
                )
            },
            logLevel: "silent"
        })
        await build({
            entryPoints: [path.join(directory, "index.ts")],
            bundle: true,
            platform: "node",
            format: "esm",
            write: false,
            external: ["durable-actors/generated"],
            logLevel: "silent"
        })
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test("regenerates typed descriptors without stale validators or server imports", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-client-"))
    try {
        for (const suffix of ["validators.js", "validators.d.ts", "proxy-validators.js", "proxy-validators.d.ts"])
            await writeFile(path.join(directory, `Room.${suffix}`), "old generated validator")
        const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
        await generateClient(
            [
                {
                    version: 1,
                    actorName: "Room",
                    emittable: ["count"],
                    schema: {
                        definitions: {
                            Metadata: { type: "object" },
                            Incoming: {
                                type: "object",
                                properties: { amount: { type: "number" } },
                                required: ["amount"]
                            },
                            Outgoing: { type: "string" },
                            State: { type: "object", properties: { count: { type: "number" } }, required: ["count"] }
                        }
                    }
                }
            ],
            directory
        )
        const source = await readFile(path.join(directory, "index.ts"), "utf8")
        assert.doesNotMatch(source, /durable-actors\/browser|createClient|export const clients/)
        assert.match(source, /amount: number/)
        assert.doesNotMatch(source, /node:|\/host|actor-compiler/)
        assert.deepEqual((await readdir(directory)).sort(), ["index.ts"])
        assert.doesNotMatch(source, /validators/)
        assert.match(await readFile(path.join(directory, "index.ts"), "utf8"), /export const actors/)
        const consumer = path.join(directory, "consumer.ts")
        await writeFile(
            consumer,
            `import { actors } from "./index.js"
            const metadata: actors.Room.Metadata = {}
            const incoming: actors.Room.Incoming = { amount: 1 }
            const state: actors.Room.State = { count: 1 }
            actors.Room.prepareWebsocket({ actorId: "lobby", metadata })
            // @ts-expect-error wrong incoming message
            const invalid: actors.Room.Incoming = { amount: "invalid" }
            // @ts-expect-error private field is absent
            state.secret
            // @ts-expect-error socket-only contracts have no RPC stub
            actors.Room.get("lobby")`
        )
        const browser = fileURLToPath(new URL("../../../../src/generated.browser.ts", import.meta.url))
        checkTypes(consumer)
        const bundle = await build({
            entryPoints: [path.join(directory, "index.ts")],
            bundle: true,
            platform: "browser",
            format: "esm",
            write: false,
            alias: { "durable-actors/generated": browser },
            metafile: true
        })
        assert.equal(
            Object.keys(bundle.metafile!.inputs).some(file =>
                /\/host\/|\/compiler\/|\/client\/|proxy|node:|ajv|validators/.test(file)
            ),
            false
        )
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test("each actor module exposes complete unprefixed contract types", async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-readable-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
    await generateClient(
        ["Counter", "Room"].map(actorName => ({
            version: 1 as const,
            actorName,
            emittable: ["count"],
            schema: {
                definitions: {
                    Metadata: { type: "object", properties: { userId: { type: "string" } }, required: ["userId"] },
                    Incoming: { type: "object", properties: { by: { type: "number" } }, required: ["by"] },
                    Outgoing: { type: "object", properties: { count: { type: "number" } }, required: ["count"] },
                    Field_count: { type: "number" },
                    State: {
                        type: "object",
                        properties: { count: { $ref: "#/definitions/Field_count" } },
                        required: ["count"]
                    }
                }
            }
        })),
        directory
    )
    const source = await readFile(path.join(directory, "index.ts"), "utf8")
    for (const name of ["Counter", "Room"]) assert.ok(source.includes(`export namespace ${name} {`))
    for (const type of ["Metadata", "Incoming", "Outgoing", "State"])
        assert.ok(source.includes(`export interface ${type} {`))
    assert.match(source, /count: number/)
    assert.doesNotMatch(source, /ActorNames|FieldCount/)
    assert.doesNotMatch(source, /ActorConnection|createClient|export const clients/)
    assert.match(source, /metadata: Metadata/)
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `
        import type { actors } from "./index.js"
        type Metadata = actors.Counter.Metadata
        type Incoming = actors.Counter.Incoming
        type Outgoing = actors.Counter.Outgoing
        type State = actors.Counter.State
        type RoomMetadata = actors.Room.Metadata
        type Authorization = actors.Room.Authorization
        const metadata: Metadata = { userId: "alice" }
        const other: RoomMetadata = metadata
        const incoming: Incoming = { by: 1 }
        const outgoing: Outgoing = { count: 1 }
        const state: State = outgoing
        const authorization: Authorization = { actorName: "Room", actorId: "lobby", metadata: other }
        // @ts-expect-error invalid metadata
        const invalid: Metadata = { userId: 1 }
        // @ts-expect-error wrong actor identity
        const wrong: Authorization = { actorName: "Counter", actorId: "one", metadata }
    `
    )
    checkTypes(consumer)
})

test("readable contract types preserve recursive metadata and helper-name collisions", async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-recursive-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const { generateClient } = await import("../../../src/compiler/generators/client-generator.js")
    await generateClient(
        [
            {
                version: 1,
                actorName: "Room",
                emittable: [],
                schema: {
                    definitions: {
                        Metadata: {
                            type: "object",
                            properties: { connection: { $ref: "#/definitions/Connection" } },
                            required: ["connection"]
                        },
                        Connection: {
                            type: "object",
                            properties: { parent: { $ref: "#/definitions/Connection" }, id: { type: "string" } },
                            required: ["id"]
                        },
                        Incoming: { type: "null" },
                        Outgoing: true,
                        State: { type: "object" }
                    }
                }
            }
        ],
        directory
    )
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `
        import type { actors } from "./index.js"
        type Metadata = actors.Room.Metadata
        type Incoming = actors.Room.Incoming
        type Outgoing = actors.Room.Outgoing
        type Authorization = actors.Room.Authorization
        const metadata: Metadata = { connection: { id: "a", parent: { id: "b" } } }
        const authorization: Authorization = { actorName: "Room", actorId: "one", metadata }
        const incoming: Incoming = null
        const outgoing: Outgoing = { anything: true }
        // @ts-expect-error recursive metadata keeps its required fields
        const invalid: Metadata = { connection: { id: "a", parent: {} } }
        // @ts-expect-error null input accepts no payload
        const wrong: Incoming = "wrong"
    `
    )
    checkTypes(consumer)
})

function checkTypes(consumer: string): void {
    const options: ts.CompilerOptions = {
        strict: true,
        noEmit: true,
        skipLibCheck: true,
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
        paths: {
            "durable-actors/generated": [fileURLToPath(new URL("../../../../src/generated.ts", import.meta.url))],
            "durable-actors/proxy": [fileURLToPath(new URL("../../../../src/proxy.ts", import.meta.url))]
        }
    }
    const program = ts.createProgram([consumer], options)
    assert.deepEqual(
        ts
            .getPreEmitDiagnostics(program)
            .map(diagnostic => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")),
        []
    )
}
