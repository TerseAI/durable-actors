import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
const sdk = path.join(root, "sdk");
process.env.DURABLE_ACTORS_TELEMETRY = "0";
const { RemoteActorClient } = await import(new URL("../../sdk/dist/client/remoteClient.js", import.meta.url));
const binary = path.resolve(process.argv[2] ?? path.join(root, "target/debug/durable-actors"));
const trials = Number(process.argv[3] ?? 3);
assert.ok(Number.isInteger(trials) && trials > 0 && trials <= 10, "trials must be 1–10");

for (let trial = 0; trial < trials; trial++) {
    const project = await mkdtemp(path.join(sdk, ".startup-benchmark-"));
    let runtime;
    let exited;
    let deadline;
    try {
        await writeFile(path.join(project, "actors.ts"), `import { Actor, Persisted } from ${JSON.stringify(path.join(sdk, "dist/index.js"))};
export class Counter extends Actor { @Persisted count = 0; async increment() { return ++this.count; } }`);
        await writeFile(path.join(project, "tsconfig.json"), JSON.stringify({ compilerOptions: { target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true, skipLibCheck: true }, include: ["actors.ts"] }));
        runtime = spawn(binary, ["dev", "--project-id", "benchmark", "--project", project, "--entrypoint", "actors.ts", "--port", "0", "--ready-fd", "3", "--sdk-host", path.join(sdk, "dist/host.js")], {
            env: { ...process.env, DURABLE_ACTORS_PARENT_LIFETIME_STDIN: "1", RUST_LOG: "error" },
            stdio: ["pipe", "pipe", "inherit", "pipe"],
        });
        exited = once(runtime, "exit");
        deadline = setTimeout(() => runtime.kill("SIGTERM"), 90_000);
        let logs = "";
        runtime.stdout.on("data", chunk => { logs = (logs + chunk).slice(-65_536); });
        const chunks = [];
        for await (const chunk of runtime.stdio[3]) chunks.push(chunk);
        assert.ok(chunks.length > 0, `runtime exited before readiness: ${logs}`);
        const connection = JSON.parse(Buffer.concat(chunks).toString());
        const settings = { projectId: connection.projectId, controlPlaneUrl: connection.controlPlaneUrl, apiKey: connection.apiKey };
        async function invoke(id, expected = 1) {
            const start = performance.now();
            assert.equal(await new RemoteActorClient(settings).invoke("Counter", id, "increment", []), expected);
            return Math.round(performance.now() - start);
        }
        const coldMs = await invoke("warm");
        const warmMs = await invoke("warm", 2);
        const burst = Promise.all(Array.from({ length: 4 }, (_, i) => invoke(`burst-${i}`)));
        await new Promise(resolve => setTimeout(resolve, 100));
        const warmDuringBurstMs = await invoke("warm", 3);
        const burstMs = await burst;
        console.log(JSON.stringify({ trial: trial + 1, coldMs, warmMs, burstMs, warmDuringBurstMs }));
        runtime.stdin.end();
        assert.equal((await exited)[0], 0);
    } finally {
        clearTimeout(deadline);
        if (runtime && runtime.exitCode === null && runtime.signalCode === null) {
            runtime.kill("SIGTERM");
            await exited;
        }
        await rm(project, { recursive: true, force: true });
    }
}
