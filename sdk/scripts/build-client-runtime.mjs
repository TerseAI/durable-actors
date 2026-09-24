import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import ts from "typescript"

const root = path.resolve("src/client-runtime")
const sources = (await readdir(root)).filter(name => name.endsWith(".ts")).sort()
const program = ts.createProgram(
    sources.map(name => path.join(root, name)),
    {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.NodeNext,
        strict: true,
        types: [],
        declaration: true,
        rootDir: root,
        outDir: "runtime"
    }
)
const diagnostics = ts.getPreEmitDiagnostics(program)
if (diagnostics.length) throw new Error(ts.formatDiagnosticsWithColorAndContext(diagnostics, ts.createCompilerHost({})))
const files = {}
const result = program.emit(undefined, (file, contents) => {
    files[file] = contents
})
if (result.emitSkipped || result.diagnostics.length) throw new Error("Could not compile the standalone client runtime")
files["runtime/package.json"] =
    JSON.stringify(
        {
            private: true,
            type: "module",
            sideEffects: false,
            browser: { "./index.js": "./index.browser.js" }
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
