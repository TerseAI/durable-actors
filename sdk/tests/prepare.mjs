import { copyFile, mkdir, writeFile } from "node:fs/promises"

await mkdir(".test-dist/tests/fixtures", { recursive: true })
await writeFile(".test-dist/package.json", JSON.stringify({ name: "durable-actors", type: "module", exports: { ".": "./src/index.js" } }))
await copyFile("tests/fixtures/actorSession.ts", ".test-dist/tests/fixtures/actorSession.ts")
await copyFile("../proto/durable_actors.proto", ".test-dist/tests/fixtures/durable_actors.proto")
