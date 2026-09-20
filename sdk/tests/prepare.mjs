import { copyFile, mkdir, writeFile } from "node:fs/promises"

await mkdir(".test-dist/tests/fixtures", { recursive: true })
await writeFile(".test-dist/package.json", JSON.stringify({ name: "little-actors", type: "module", exports: { ".": "./src/index.js" } }))
await copyFile("tests/fixtures/actorSession.ts", ".test-dist/tests/fixtures/actorSession.ts")
