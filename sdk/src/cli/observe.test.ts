import assert from "node:assert/strict"
import { cp, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { get } from "node:http"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { pathToFileURL } from "node:url"

import { Observer } from "./observe.js"

const assets = new URL("../../../dist/observer/", import.meta.url)

test("observer serves generated assets without a hardcoded filename list", async t => {
    const directory = await mkdtemp(join(tmpdir(), "observer-assets-"))
    await cp(assets, directory, { recursive: true })
    await mkdir(join(directory, "assets"))
    await writeFile(join(directory, "assets", "details-abc123.js"), "export const details = true")
    const observer = new Observer(
        { checkConnection: async () => {}, listActors: async () => ({ namespaceId: "local", actors: [] }) },
        async () => {},
        pathToFileURL(`${directory}/`)
    )
    t.after(async () => {
        await observer.close()
        await rm(directory, { recursive: true, force: true })
    })
    const { url } = await observer.start(false)
    const response = await fetch(`${url}/assets/details-abc123.js`)
    assert.equal(response.status, 200)
    assert.match(response.headers.get("content-type")!, /javascript/u)
    assert.equal(await response.text(), "export const details = true")
    const head = await fetch(`${url}/assets/details-abc123.js`, { method: "HEAD" })
    assert.equal(head.status, 200)
    assert.equal(await head.text(), "")
})

test("observer proxies connection checks and reports a later outage without exposing upstream errors", async t => {
    let available = true
    const observer = new Observer(
        {
            listActors: async () => ({ namespaceId: "local", actors: [] }),
            checkConnection: async () => {
                if (!available) throw new Error("secret-admin-key")
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    const connected = await fetch(`${url}/api/observe/connection`)
    assert.equal(connected.status, 200)
    assert.deepEqual(await connected.json(), { connected: true })
    available = false
    const disconnected = await fetch(`${url}/api/observe/connection`)
    assert.equal(disconnected.status, 503)
    assert.deepEqual(await disconnected.json(), { error: "Control plane connection failed" })
    assert.equal((await fetch(`${url}/api/observe/connection`, { method: "POST" })).status, 405)
    assert.equal(
        (await fetch(`${url}/api/observe/connection`, { headers: { origin: "https://untrusted.example" } })).status,
        403
    )
    const rejectedHost = await new Promise<number | undefined>((resolve, reject) => {
        get(`${url}/api/observe/connection`, { headers: { host: "untrusted.example" } }, response => {
            response.resume()
            resolve(response.statusCode)
        }).on("error", reject)
    })
    assert.equal(rejectedHost, 403)
})

test("observer opens the browser only after authentication and local serving succeed", async t => {
    let connected = false
    let opened: string | undefined
    const observer = new Observer(
        {
            listActors: async () => ({ namespaceId: "local", actors: [] }),
            checkConnection: async () => {
                connected = true
            }
        },
        async url => {
            assert.equal(connected, true)
            const response = await fetch(url)
            assert.match(response.headers.get("content-type")!, /^text\/html(?:;|$)/u)
            assert.match(await response.text(), /src="\.\/app.js"/u)
            assert.match(await (await fetch(`${url}/app.js`)).text(), /No actors yet/u)
            assert.match((await fetch(`${url}/app.css`)).headers.get("content-type")!, /text\/css/u)
            opened = url
        },
        assets
    )
    t.after(() => observer.close())
    const result = await observer.start()
    assert.equal(opened, result.url)
    assert.equal(result.browserOpened, true)
    assert.equal((await fetch(`${result.url}/missing`)).status, 404)
    assert.equal((await fetch(`${result.url}/package.json`)).status, 404)
})

test("observer does not open a browser when the control plane rejects the connection", async () => {
    let opened = false
    const observer = new Observer(
        {
            listActors: async () => ({ namespaceId: "local", actors: [] }),
            checkConnection: async () => {
                throw new Error("Unauthorized")
            }
        },
        async () => {
            opened = true
        }
    )
    await assert.rejects(observer.start(), /Unauthorized/u)
    assert.equal(opened, false)
})

test("observer keeps the local UI available if browser launching fails", async t => {
    const observer = new Observer(
        { checkConnection: async () => {}, listActors: async () => ({ namespaceId: "local", actors: [] }) },
        async () => {
            throw new Error("No browser")
        },
        assets
    )
    t.after(() => observer.close())
    const result = await observer.start()
    assert.equal(result.browserOpened, false)
    assert.equal((await fetch(result.url)).status, 200)
})

test("observer proxies actor inventory and hides upstream failures", async t => {
    let fail = false
    const inventory = { namespaceId: "team", actors: [{ actorType: "Room", live: 1, dormant: 2, unknown: 0 }] }
    const observer = new Observer(
        {
            checkConnection: async () => {},
            listActors: async () => {
                if (fail) throw new Error("private-admin-key")
                return inventory
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    assert.deepEqual(await (await fetch(`${url}/api/observe/actors`)).json(), inventory)
    fail = true
    const response = await fetch(`${url}/api/observe/actors`)
    assert.equal(response.status, 503)
    assert.deepEqual(await response.json(), { error: "Actor inventory unavailable" })
})

test("observer forwards live events and aborts upstream when the viewer disconnects", async t => {
    let signal: AbortSignal | undefined
    const observer = new Observer(
        {
            checkConnection: async () => {},
            listActors: async () => ({}),
            openActorStream: async incoming => {
                signal = incoming
                return new Response(
                    new ReadableStream({
                        start(controller) {
                            controller.enqueue(
                                new TextEncoder().encode(
                                    'event: inventory\ndata: {"namespaceId":"local","actors":[]}\n\n'
                                )
                            )
                        }
                    }),
                    { headers: { "content-type": "text/event-stream" } }
                )
            }
        },
        async () => {},
        assets
    )
    t.after(() => observer.close())
    const { url } = await observer.start(false)
    const controller = new AbortController()
    const response = await fetch(`${url}/api/observe/events`, { signal: controller.signal })
    assert.equal(response.status, 200)
    assert.match(response.headers.get("content-type")!, /text\/event-stream/u)
    const reader = response.body!.getReader()
    assert.match(new TextDecoder().decode((await reader.read()).value), /event: inventory/u)
    const stopped = new Promise<void>(resolve => signal!.addEventListener("abort", () => resolve(), { once: true }))
    controller.abort()
    await stopped
    assert.equal(signal!.aborted, true)
})
