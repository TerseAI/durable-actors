import { build } from "esbuild"
import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"

test("the public package bundles into a hosted browser app without a second React or Node runtime", async () => {
    const result = await build({
        stdin: { contents: 'export { ActorObserver, HttpObserverClient } from "durable-actors-observer"; import "durable-actors-observer/styles.css"', resolveDir: process.cwd() },
        outfile: "consumer.js",
        bundle: true,
        platform: "browser",
        format: "esm",
        external: ["react", "react/*", "react-dom", "react-dom/*"],
        write: false,
        metafile: true
    })
    const inputs = Object.keys(result.metafile.inputs)
    assert.ok(inputs.includes("dist/index.js"))
    assert.ok(inputs.includes("dist/styles.css"))
    assert.ok(inputs.every(input => !input.includes("node_modules/react/") && !input.includes("node_modules/react-dom/") && !input.startsWith("src/")))
    assert.ok(
        Object.values(result.metafile.outputs)
            .flatMap(output => output.imports)
            .some(imported => imported.path === "react" && imported.external)
    )
    assert.match(await readFile("dist/index.d.ts", "utf8"), /ObserverClient/u)
})

test("the standalone package contains its browser entry and styles", async () => {
    const html = await readFile("dist/standalone/index.html", "utf8")
    for (const [, file] of html.matchAll(/(?:src|href)="\.\/([^"]+)"/gu)) {
        assert.ok((await readFile(`dist/standalone/${file}`)).length > 0)
    }
    const application = await readFile("dist/standalone/app.js", "utf8")
    assert.doesNotMatch(application, /Keep the observe command running/u)
})

test("embedded styles inherit host tokens while standalone ships the neutral light and dark palette", async () => {
    const styles = await readFile("dist/styles.css", "utf8")
    assert.match(styles, /var\(--foreground\)/u)
    assert.match(styles, /var\(--primary\)/u)
    assert.doesNotMatch(styles, /--(?:background|foreground|primary):/u)
    assert.doesNotMatch(styles, /--la-observer-/u)
    const theme = await readFile("dist/theme.css", "utf8")
    assert.match(theme, /--background:\s*#fafafa[;}]/u)
    assert.match(theme, /--background:\s*#0a0a0a[;}]/u)
    const standalone = await readFile("dist/standalone/app.css", "utf8")
    assert.match(standalone, /#fafafa/iu)
    assert.match(standalone, /var\(--primary\)/u)
    assert.doesNotMatch(standalone, /padding-left: 0;/u)
})
