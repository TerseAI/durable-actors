import assert from "node:assert/strict"
import { test } from "node:test"
import { build } from "vite"

test("Vite builds the embeddable library with separate styles and external runtime dependencies", async () => {
    const result = await build({ mode: "library", logLevel: "silent", build: { write: false } })
    assert.ok(!("on" in result))
    const output = (Array.isArray(result) ? result : [result]).flatMap(bundle => bundle.output)
    const entry = output.find(file => file.fileName === "index.js")
    assert.ok(entry?.type === "chunk")
    assert.ok(entry.imports.includes("react"))
    assert.ok(entry.imports.includes("lucide-react"))
    assert.ok(entry.imports.includes("eventsource-parser"))
    assert.ok(output.some(file => file.fileName === "styles.css"))
    assert.ok(output.some(file => file.fileName === "theme.css"))
    assert.ok(!output.some(file => file.fileName.endsWith(".html")))
})
