import assert from "node:assert/strict"
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"

import { startLocalActors } from "./localRuntime.js"

async function fixture(source: string, run: (binary: string) => Promise<void>) {
    const directory = await mkdtemp(path.join(os.tmpdir(), "local-runtime-"))
    const binary = path.join(directory, "runtime")
    const previous = process.env.DURABLE_OBJECT_BINARY
    await writeFile(binary, `#!${process.execPath}\n${source}`)
    await chmod(binary, 0o755)
    process.env.DURABLE_OBJECT_BINARY = binary
    try {
        await run(binary)
    } finally {
        if (previous === undefined) delete process.env.DURABLE_OBJECT_BINARY
        else process.env.DURABLE_OBJECT_BINARY = previous
        await rm(directory, { recursive: true, force: true })
    }
}

test("waits for the readiness pipe and stops idempotently", async () => {
    await fixture(
        `const fs = require("node:fs");
        const fd = Number(process.argv[process.argv.indexOf("--ready-fd") + 1]);
        fs.writeSync(fd, '{"controlPlaneUrl":"http://127.0.0.1:7100",');
        setTimeout(() => {
            fs.writeSync(fd, '"apiKey":"secret","namespaceId":"local","storageRegion":"local","pid":' + process.pid + '}');
            fs.closeSync(fd);
        }, 20);
        process.stdin.resume();
        process.stdin.on("end", () => process.exit(0));`,
        async () => {
            const runtime = await startLocalActors({ entrypoint: "src/actors.ts" })
            assert.equal(runtime.connection.namespaceId, "local")
            assert.equal(runtime.connection.apiKey, "secret")
            await Promise.all([runtime.stop(), runtime.stop()])
            await runtime.closed
            assert.throws(() => process.kill(runtime.connection.pid, 0), { code: "ESRCH" })
        }
    )
})

test("rejects an early process exit", async () => {
    await fixture("process.exit(2)", async () => {
        await assert.rejects(startLocalActors({ entrypoint: "src/actors.ts" }), /exited|readiness/)
    })
})

test("times out and reaps a runtime that never becomes ready", async () => {
    await fixture(
        `require("node:fs").writeFileSync(__filename + ".pid", String(process.pid));
        process.stdin.resume(); process.stdin.on("end", () => process.exit(0));`,
        async binary => {
            await assert.rejects(startLocalActors({ entrypoint: "src/actors.ts", startupTimeoutMs: 500 }), /timed out/i)
            const pid = Number(await readFile(`${binary}.pid`, "utf8"))
            assert.throws(() => process.kill(pid, 0), { code: "ESRCH" })
        }
    )
})

test("rejects malformed readiness and stops the process", async () => {
    await fixture(
        `const fs = require("node:fs");
        fs.writeFileSync(__filename + ".pid", String(process.pid));
        fs.writeSync(3, "invalid JSON"); fs.closeSync(3);
        process.stdin.resume(); process.stdin.on("end", () => process.exit(0));`,
        async binary => {
            await assert.rejects(startLocalActors({ entrypoint: "src/actors.ts" }), SyntaxError)
            const pid = Number(await readFile(`${binary}.pid`, "utf8"))
            assert.throws(() => process.kill(pid, 0), { code: "ESRCH" })
        }
    )
})
