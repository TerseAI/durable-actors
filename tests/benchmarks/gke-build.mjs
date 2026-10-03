import { createHash, randomUUID } from "node:crypto"
import { mkdir, readFile, writeFile } from "node:fs/promises"
import { resolve } from "node:path"

import { buildActor } from "../../sdk/dist/compiler/actor-build.js"
import { build } from "../../sdk/node_modules/esbuild/lib/main.js"

const directory = resolve(process.argv[2])
await mkdir(directory, { recursive: true })
const contract = await buildActor(resolve("tests/benchmarks/actors.ts"), `${directory}/actors.mjs`, {
    configFile: resolve("tests/benchmarks/tsconfig.json")
})
await writeFile(`${directory}/contract.json`, JSON.stringify(contract))
await build({
    entryPoints: ["tests/benchmarks/gke-worker.mjs"],
    outfile: `${directory}/worker.mjs`,
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node22",
    banner: { js: "import { createRequire } from 'node:module'; const require = createRequire(import.meta.url);" }
})
const bytes = await readFile(`${directory}/actors.mjs`)
await writeFile(
    `${directory}/artifact.json`,
    JSON.stringify({
        path: "actors.mjs",
        object: `durable-actors/v3/artifacts/${randomUUID().replaceAll("-", "")}/actors.mjs`,
        sha256: createHash("sha256").update(bytes).digest("base64url")
    })
)
console.log(directory)
