import { build } from "esbuild"
import { readFile } from "node:fs/promises"
import { createRequire } from "node:module"
import path from "node:path"

const require = createRequire(import.meta.url)
const generator = path.dirname(require.resolve("dts-bundle-generator/package.json"))
const license = await readFile(path.join(generator, "LICENSE"), "utf8")

// The generator accepts TypeScript >=5, but TypeScript 7 removed its compiler API.
// Bundle it here so it uses the SDK's pinned TypeScript instead of a separate install.
await build({
    entryPoints: ["src/compiler/declarations.ts"],
    outfile: "dist/compiler/declarations.js",
    bundle: true,
    platform: "node",
    format: "esm",
    external: ["typescript", "../errors.js"],
    sourcemap: true,
    banner: {
        js: `/*! dts-bundle-generator\n${license}*/\nimport { createRequire } from "node:module"; const require = createRequire(import.meta.url);`
    }
})
