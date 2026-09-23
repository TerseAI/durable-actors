import assert from "node:assert/strict"
import { execFile, spawn } from "node:child_process"
import { once } from "node:events"
import { cp, mkdir, mkdtemp, readFile, rm, symlink } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { generateClient } from "../../dist/compiler/generators/client-generator.js"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../", import.meta.url))
const next = path.join(sdk, "node_modules/next/dist/bin/next")
const env = { ...process.env, NEXT_TELEMETRY_DISABLED: "1", FORCE_COLOR: "0" }

for (const bundler of ["--turbopack", "--webpack"])
    test(`generated clients work in Next.js development and production with typed methods (${bundler})`, { timeout: 180_000 }, async t => {
        const directory = await nextProject(t)
        const dev = await startNext(t, directory, "dev", bundler)
        await checkResponse(dev.origin)
        await dev.stop()
        try {
            await run(process.execPath, [next, "build", bundler], { cwd: directory, env, timeout: 120_000 })
        } catch (error) {
            assert.fail(`${error.stdout}\n${error.stderr}`)
        }
        const production = await startNext(t, directory, "start")
        await checkResponse(production.origin)
        await production.stop()
    })

async function nextProject(t) {
    const parent = path.join(sdk, "../.artifacts/next-clients")
    await mkdir(parent, { recursive: true })
    const directory = await mkdtemp(path.join(parent, "next-client-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await cp(new URL("../fixtures/next-client/", import.meta.url), directory, { recursive: true })
    await symlink(path.join(sdk, "node_modules"), path.join(directory, "node_modules"), "dir")
    const contract = JSON.parse(await readFile(new URL("../fixtures/public-contract.json", import.meta.url), "utf8"))
    await generateClient(contract, path.join(directory, "generated"))
    return directory
}

async function startNext(t, directory, command, bundler) {
    const child = spawn(process.execPath, [next, command, "--hostname", "127.0.0.1", "--port", "0", ...(bundler ? [bundler] : [])], {
        cwd: directory,
        env,
        stdio: ["ignore", "pipe", "pipe"]
    })
    const exited = once(child, "exit")
    const stop = async () => {
        if (child.exitCode !== null || child.signalCode !== null) return
        child.kill()
        await exited
    }
    t.after(stop)
    let output = ""
    const ready = new Promise((resolve, reject) => {
        const receive = chunk => {
            output += chunk.toString()
            const origin = output.match(/http:\/\/(?:localhost|127\.0\.0\.1):\d+/)?.[0]
            if (origin && /Ready in/.test(output)) resolve(origin)
        }
        child.stdout.on("data", receive)
        child.stderr.on("data", receive)
        child.on("error", reject)
    })
    const origin = await Promise.race([
        ready,
        exited.then(() => {
            throw new Error(`Next.js ${command} exited before becoming ready:\n${output}`)
        })
    ])
    return { origin, stop }
}

async function checkResponse(origin) {
    const response = await fetch(`${origin}/api/actor`, { signal: AbortSignal.timeout(30_000) })
    const body = await response.text()
    assert.equal(response.status, 200, body)
    assert.deepEqual(JSON.parse(body), { id: "ChatRoom/lobby/sendMessage", text: "hello" })
    const page = await fetch(origin, { signal: AbortSignal.timeout(30_000) })
    assert.equal(page.status, 200)
    assert.match(await page.text(), /Standalone client/)
}
