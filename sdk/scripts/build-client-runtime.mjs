import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises"

const files = {}
const { dependencies } = JSON.parse(await readFile("package.json", "utf8"))
for (const name of (await readdir("src/client-runtime")).sort()) {
    if (name.endsWith(".ts")) files[`runtime/${name}`] = await readFile(`src/client-runtime/${name}`, "utf8")
}
files["runtime/package.json"] =
    JSON.stringify(
        {
            private: true,
            type: "module",
            sideEffects: false,
            dependencies: { zod: dependencies.zod },
            browser: { "./index.ts": "./index.browser.ts", "./index.js": "./index.browser.js" }
        },
        null,
        2
    ) + "\n"
files["runtime/LICENSE.md"] = await readFile("LICENSE.md", "utf8")
await rm("src/generated", { recursive: true, force: true })
await mkdir("src/generated", { recursive: true })
await writeFile(
    "src/generated/client-runtime.ts",
    `export const runtimeFiles: Record<string, string> = ${JSON.stringify(files)}\n`
)
