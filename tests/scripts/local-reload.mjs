import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const sdk = fileURLToPath(new URL("../../sdk/", import.meta.url));
process.env.DURABLE_ACTORS_TELEMETRY = "0";
const { RemoteActorClient } = await import(new URL("../../sdk/dist/client/remoteClient.js", import.meta.url));
const project = await mkdtemp(path.join(sdk, ".reload-test-"));
const source = `import { Actor, Persisted } from "durable-actors";
import { label } from "./label.js";
export class Counter extends Actor {
    @Persisted count = 0;
    async increment(): Promise<number> { return ++this.count; }
    async read(): Promise<number> { return this.count; }
    async label(): Promise<string> { return label; }
}`;
let runtime;
let exited;
let logs = "";
async function waitFor(pattern, offset = 0) {
    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline) {
        const found = logs.slice(offset).match(pattern);
        if (found) return found;
        if (runtime.exitCode !== null) break;
        await delay(20);
    }
    throw new Error(`Missing ${pattern}: ${logs}`);
}
try {
    await writeFile(path.join(project, "actors.ts"), source);
    await writeFile(path.join(project, "label.ts"), 'export const label: string = "before";');
    await writeFile(path.join(project, "tsconfig.json"), JSON.stringify({ compilerOptions: { target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true, skipLibCheck: true }, include: ["*.ts"] }));
    runtime = spawn(process.execPath, [path.join(sdk, "dist/cli.js"), "dev", "--port", "0"], {
        cwd: project,
        env: { ...process.env, DURABLE_ACTORS_BINARY: path.resolve(process.argv[2]), DURABLE_ACTORS_PROJECT: project, DURABLE_ACTORS_PROJECT_ID: "local", DURABLE_ACTORS_ENTRYPOINT: "actors.ts", DURABLE_ACTORS_SECRET: "reload-test", DURABLE_ACTORS_STORAGE: "local", DURABLE_ACTORS_DATA_DIR: path.join(project, ".durable-actors"), DURABLE_ACTORS_TELEMETRY: "0", RUST_LOG: "error" },
        stdio: ["pipe", "pipe", "pipe"],
    });
    exited = once(runtime, "exit");
    for (const stream of [runtime.stdout, runtime.stderr]) stream.on("data", chunk => { logs += chunk; });
    const ready = await waitFor(/Ready\s+(http:\/\/127\.0\.0\.1:\d+)/);
    const settings = { projectId: "local", controlPlaneUrl: ready[1], apiKey: "reload-test" };
    const client = new RemoteActorClient(settings);
    const invoke = (id, method) => client.invoke("Counter", id, method, []);
    assert.equal(await invoke("saved", "increment"), 1);
    assert.equal(await invoke("saved", "label"), "before");
    let offset = logs.length;
    const reloadStarted = performance.now();
    await writeFile(path.join(project, "label.ts"), 'export const label: string = "after";');
    await waitFor(/Updated local actors\./, offset);
    const reloadMs = Math.round(performance.now() - reloadStarted);
    await assert.rejects(invoke("saved", "label"), { code: "outcome_unknown" });
    assert.equal(await invoke("saved", "label"), "after");
    assert.equal(await invoke("saved", "read"), 1);
    offset = logs.length;
    await writeFile(path.join(project, "actors.ts"), "invalid TypeScript");
    await waitFor(/Actor source update failed:/, offset);
    assert.equal(await invoke("fresh-after-error", "label"), "after");
    assert.equal(await invoke("saved", "read"), 1);
    offset = logs.length;
    await writeFile(path.join(project, "actors.ts"), source);
    await waitFor(/Updated local actors\./, offset);
    await assert.rejects(invoke("saved", "read"), { code: "outcome_unknown" });
    assert.equal(await invoke("saved", "read"), 1);
    assert.equal((await readdir(path.join(project, ".durable-actors/code/local"))).length, 1);
    console.log(JSON.stringify({ reloadMs, persistedState: "preserved", failedBuild: "kept current code", retainedBuilds: 1, cachedRoute: "invalidated after transport failure" }));
} finally {
    if (runtime && runtime.exitCode === null && runtime.signalCode === null) {
        runtime.kill("SIGTERM");
        const killDeadline = setTimeout(() => runtime.kill("SIGKILL"), 10_000);
        await exited;
        clearTimeout(killDeadline);
    }
    await rm(project, { recursive: true, force: true });
}
