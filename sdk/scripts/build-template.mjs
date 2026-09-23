import { copyFile, cp, mkdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "node:path"

const { version } = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"))

for (const template of ["actor", "chat", "ai-chat", "documents"]) await buildTemplate(template)

async function buildTemplate(template) {
    const source = new URL(
        template === "actor" ? "../templates/actor/" : `../../examples/${template}/`,
        import.meta.url
    )
    const destination = new URL(`../dist/templates/${template}/`, import.meta.url)
    await rm(destination, { recursive: true, force: true })
    await mkdir(destination, { recursive: true })
    const files = ["package.json", "tsconfig.json", "README.md", "src"]
    if (template !== "actor") files.push("index.html")
    if (template !== "actor") files.push(".env.example")
    for (const file of files)
        await cp(new URL(file, source), new URL(file, destination), {
            recursive: true,
            filter: file => !["generated", "node_modules", ".durable-actors", "dist"].includes(path.basename(file))
        })
    // npm excludes .gitignore; init restores its name after copying the template.
    await copyFile(new URL(".gitignore", source), new URL("gitignore", destination))
    await copyFile(new URL("../../LICENSE.md", import.meta.url), new URL("LICENSE.md", destination))
    const metadata = JSON.parse(await readFile(new URL("package.json", destination), "utf8"))
    metadata.dependencies["durable-actors"] = version
    await writeFile(new URL("package.json", destination), JSON.stringify(metadata, null, 4) + "\n")
}
