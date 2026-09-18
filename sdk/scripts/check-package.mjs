import { access, readFile } from "node:fs/promises"

await checkPackage()

async function checkPackage() {
    const metadata = JSON.parse(await readFile("package.json", "utf8"))
    if (metadata.name !== "little-actors") throw new Error("unexpected package name")
    if (metadata.license !== "MIT") throw new Error("package license must be MIT")
    for (const entry of Object.values(metadata.exports)) await checkExport(entry)
    const cli = metadata.bin?.["little-actors"]
    if (
        Object.keys(metadata.bin ?? {}).join() !== "little-actors" ||
        !cli ||
        !(await readFile(cli, "utf8")).startsWith("#!/usr/bin/env node\n")
    )
        throw new Error("package must expose little-actors as its executable CLI")
    await access("dist/generated/durable_object.proto")
    for (const asset of ["index.html", "app.js", "app.css"]) await access(`dist/observer/${asset}`)
    for (const name of ["chat", "ai-chat", "documents"]) {
        const template = JSON.parse(await readFile(`dist/templates/${name}/package.json`, "utf8"))
        if (template.dependencies["little-actors"] !== metadata.version)
            throw new Error(`${name} template must use the packaged SDK version`)
        await access(`dist/templates/${name}/gitignore`)
    }
    await import("../dist/client/actorHostGrpc.js")
    await Promise.all(metadata.files.filter(path => !path.includes("dist")).map(path => access(path)))
}

async function checkExport(entry) {
    if (typeof entry !== "object" || entry === null)
        throw new Error("package exports must declare import and types paths")
    await Promise.all([access(entry.import), access(entry.types)])
    if (entry.browser) await access(entry.browser)
}
