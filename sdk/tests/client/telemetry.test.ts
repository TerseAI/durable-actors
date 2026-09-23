import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { test } from "node:test"
import { promisify } from "node:util"

for (const [setting, enabled] of [
    [undefined, false],
    ["0", false],
    ["1", true],
    ["", false],
    ["true", false]
] as const) {
    test(`client telemetry with DURABLE_ACTORS_TELEMETRY=${setting ?? "unset"} is ${enabled ? "enabled" : "disabled"}`, async () => {
        const env = { ...process.env }
        delete env.DURABLE_ACTORS_TELEMETRY
        if (setting !== undefined) env.DURABLE_ACTORS_TELEMETRY = setting
        const source = `
            import { stderrTelemetry } from ${JSON.stringify(new URL("../../src/client/telemetry.js", import.meta.url).href)};
            stderrTelemetry({ event: "actor_client_invocation" });
            console.log("workflow output");
        `
        const { stdout, stderr } = await promisify(execFile)(process.execPath, ["--input-type=module", "-e", source], {
            env
        })
        assert.equal(stdout, "workflow output\n")
        assert.equal(stderr, enabled ? '{"event":"actor_client_invocation"}\n' : "")
    })
}
