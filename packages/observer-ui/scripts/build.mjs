import tailwindcss from "@tailwindcss/postcss"
import { build } from "esbuild"
import { copyFile, mkdir, readFile, rm, writeFile } from "node:fs/promises"
import postcss from "postcss"
import { build as buildStandalone } from "vite"

await rm("dist", { recursive: true, force: true })
await mkdir("dist", { recursive: true })
await build({ entryPoints: ["src/index.ts"], outfile: "dist/index.js", bundle: true, format: "esm", platform: "browser", packages: "external", target: "es2022" })
const styles = await postcss([tailwindcss()]).process(await readFile("src/styles.css", "utf8"), { from: "src/styles.css", to: "dist/styles.css" })
await writeFile("dist/styles.css", styles.css)
await copyFile("src/theme.css", "dist/theme.css")
await buildStandalone()
