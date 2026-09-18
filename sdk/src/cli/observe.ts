import { Command } from "commander"
import { once } from "node:events"
import { readFile } from "node:fs/promises"
import { createServer } from "node:http"
import type { IncomingMessage, ServerResponse } from "node:http"
import type { AddressInfo } from "node:net"
import open from "open"

import { connection, connectionOptions } from "./connection.js"
import type { ConnectionOptions } from "./connection.js"
import { ControlPlaneClient } from "./control-plane.js"

function registerObserveCommand(program: Command): void {
    connectionOptions(program.command("observe").description("Open the local observability UI"))
        .option("--no-open", "print the UI URL without opening a browser")
        .action(async (options: ConnectionOptions & { open: boolean }) => {
            const observer = new Observer(new ControlPlaneClient(connection(options), fetch), open)
            const result = await observer.start(options.open)
            const stop = () => {
                process.off("SIGINT", stop)
                process.off("SIGTERM", stop)
                void observer.close()
            }
            process.once("SIGINT", stop)
            process.once("SIGTERM", stop)
            console.log("hello I am connected to the control plane")
            console.log(`Observe: ${result.url}`)
            console.log("Press Ctrl+C to stop.")
            if (options.open && !result.browserOpened)
                console.error("Could not open your browser. Open the URL above manually.")
        })
}

class Observer {
    private readonly assets = new Map<string, { contentType: string; body: Buffer }>()
    private readonly server = createServer((request, response) => {
        void this.respond(request, response).catch(() => response.writeHead(500).end())
    })

    constructor(
        private readonly client: Pick<ControlPlaneClient, "checkConnection" | "listActors">,
        private readonly openBrowser: (url: string) => Promise<unknown>,
        private readonly assetDirectory = new URL("../observer/", import.meta.url)
    ) {}

    async start(launchBrowser = true): Promise<{ url: string; browserOpened: boolean }> {
        await this.client.checkConnection()
        await this.loadAssets()
        this.server.listen(0, "127.0.0.1")
        await once(this.server, "listening")
        const url = `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`
        const browserOpened = launchBrowser
            ? await this.openBrowser(url).then(
                  () => true,
                  () => false
              )
            : false
        return { url, browserOpened }
    }

    async close(): Promise<void> {
        if (!this.server.listening) return
        const closed = new Promise<void>((resolve, reject) => {
            this.server.close(error => (error ? reject(error) : resolve()))
        })
        this.server.closeAllConnections()
        await closed
    }

    private async loadAssets(): Promise<void> {
        for (const [route, file, contentType] of [
            ["/", "index.html", "text/html; charset=utf-8"],
            ["/app.js", "app.js", "text/javascript; charset=utf-8"],
            ["/app.css", "app.css", "text/css; charset=utf-8"]
        ]) {
            this.assets.set(route, { contentType, body: await readFile(new URL(file, this.assetDirectory)) })
        }
    }

    private async respond(request: IncomingMessage, response: ServerResponse): Promise<void> {
        response.setHeader("cache-control", "no-store")
        response.setHeader("x-content-type-options", "nosniff")
        response.setHeader(
            "content-security-policy",
            "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        )
        const host = `127.0.0.1:${(this.server.address() as AddressInfo).port}`
        if (
            request.headers.host !== host ||
            (request.headers.origin && request.headers.origin !== `http://${host}`) ||
            request.headers["sec-fetch-site"] === "cross-site"
        ) {
            response.writeHead(403).end()
            return
        }
        if (request.method !== "GET" && request.method !== "HEAD") {
            response.writeHead(405, { allow: "GET, HEAD" }).end()
            return
        }
        const pathname = new URL(request.url!, `http://${host}`).pathname
        if (pathname === "/api/observe/actors") {
            await this.actorInventory(request, response)
            return
        }
        if (pathname === "/api/observe/connection") {
            await this.connectionStatus(request, response)
            return
        }
        const asset = this.assets.get(pathname)
        if (!asset) {
            response.writeHead(404).end()
            return
        }
        response.writeHead(200, { "content-type": asset.contentType })
        response.end(request.method === "HEAD" ? undefined : asset.body)
    }

    private async actorInventory(request: IncomingMessage, response: ServerResponse): Promise<void> {
        let status = 200
        let result: unknown
        try {
            result = await this.client.listActors()
        } catch {
            status = 503
            result = { error: "Actor inventory unavailable" }
        }
        response.writeHead(status, { "content-type": "application/json" })
        response.end(request.method === "HEAD" ? undefined : JSON.stringify(result))
    }

    private async connectionStatus(request: IncomingMessage, response: ServerResponse): Promise<void> {
        let status = 200
        let result: unknown = { connected: true }
        try {
            await this.client.checkConnection()
        } catch {
            status = 503
            result = { error: "Control plane connection failed" }
        }
        response.writeHead(status, { "content-type": "application/json" })
        response.end(request.method === "HEAD" ? undefined : JSON.stringify(result))
    }
}

export { Observer, registerObserveCommand }
