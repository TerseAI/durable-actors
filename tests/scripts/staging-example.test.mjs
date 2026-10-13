import { expect, test } from "bun:test"
import path from "node:path"
import { fileURLToPath } from "node:url"

import { Staging, stagingExamplePath } from "../../scripts/staging.mjs"

test("staging resolves grouped actor projects", () => {
    expect(stagingExamplePath("/repo", "persistence/persistence-tests")).toBe(path.join("/repo", "examples", "persistence", "persistence-tests"))
    expect(stagingExamplePath("/repo", "chat")).toBe(path.join("/repo", "examples", "chat"))
    expect(stagingExamplePath("/repo", "pi/pi-durable")).toBe(path.join("/repo", "examples", "pi", "pi-durable"))
})

test("Pi staging builds the workspace adapter before bundling and includes the model secret", async () => {
    const root = fileURLToPath(new URL("../../", import.meta.url))
    const commands = []
    const staging = new Staging(
        root,
        {
            STAGING_SECRET_KEYS: "OPENAI_API_KEY",
            OPENAI_API_KEY: "test-model-key"
        },
        stagingExamplePath(root, "pi/pi-durable"),
        async (command, args) => {
            commands.push([command, ...args])
            return "{}"
        }
    )
    staging.buildRuntimeImage = async runtime => `test-${runtime}`
    staging.saveSetting = async () => {}
    staging.uploadBundle = async () => ({ bucket: "test-artifacts", files: [] })

    const deployment = await staging.build()
    const adapter = commands.findIndex(command => command.includes("packages/pi-durable"))
    const bundle = commands.findIndex(command => command.includes(path.join(root, "sdk/dist/compiler/deployment-build.js")))
    expect(adapter).toBeGreaterThan(0)
    expect(adapter).toBeLessThan(bundle)
    expect(staging.secrets).toEqual({ OPENAI_API_KEY: "test-model-key" })
    expect(deployment.secretRefs).toEqual(["staging-pi-durable"])
})

test("staging rejects paths outside the example tree", () => {
    for (const name of [undefined, "", "../sdk", "/chat", "persistence/../chat", "persistence//cli"]) expect(() => stagingExamplePath("/repo", name)).toThrow("Usage:")
})
