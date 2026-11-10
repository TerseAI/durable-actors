import { resolve } from "import-meta-resolve"
import { realpathSync } from "node:fs"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"

import { ACTOR_ARTIFACT_VERSION, type ActorSchema } from "../actor/schema.js"

import { ActorCompiler } from "./actor-compiler.js"
import type { CompilerOptions } from "./types.js"

async function buildActor(entrypoint: string, outfile: string, options: CompilerOptions & { local?: boolean } = {}) {
    const compilation = new ActorCompiler().compileDeployment(entrypoint, options)
    await bundleActor(entrypoint, outfile, compilation, options)
    return compilation.contract
}

async function bundleActor(
    entrypoint: string,
    outfile: string,
    compilation: ReturnType<ActorCompiler["compileDeployment"]>,
    options: CompilerOptions & { local?: boolean } = {}
) {
    if (!outfile.endsWith(".mjs")) throw new Error("actor build output must use the .mjs extension")
    const source = path.resolve(entrypoint)
    const { schemas, sources } = compilation
    const artifact = path.join(path.dirname(source), ".durable-actors-artifact.ts")
    const result = await Bun.build({
        entrypoints: [artifact],
        files: { [artifact]: artifactModule(source, schemas) },
        tsconfig: options.configFile,
        external: options.local
            ? [fileURLToPath(new URL("../*", import.meta.url))]
            : ["durable-actors", "durable-actors/*"],
        plugins: [analyzedSources(sources), bundledSdkAdapters(), ...(options.local ? [localSdkImports()] : [])],
        loader: { ".sql": "text" },
        sourcemap: options.local ? "inline" : "none",
        target: "bun",
        format: "esm",
        minify: { keepNames: true },
        banner: RUNTIME_PATHS,
        define: { __dirname: "__durableActorsDirname", __filename: "__durableActorsFilename" }
    })
    await mkdir(path.dirname(outfile), { recursive: true })
    await writeFile(outfile, await result.outputs[0]!.text())
}

// Bun renames classes whose names collide with another module's top-level names, even with `keepNames`, and actors are
// registered by class name.
function artifactModule(source: string, schemas: readonly ActorSchema[]): string {
    return `import * as actors from ${JSON.stringify(source)};
        for (const name of ${JSON.stringify(schemas.map(schema => schema.actorName))})
            Object.defineProperty(actors[name], "name", { value: name });
        export { actors };
        export const schemas = ${JSON.stringify(schemas)};
        export const version = ${ACTOR_ARTIFACT_VERSION};`
}

// Bun inlines the build machine's paths for CommonJS `__dirname` and `__filename`, so they read the bundle's instead.
const RUNTIME_PATHS =
    "const __durableActorsDirname = import.meta.dirname, __durableActorsFilename = import.meta.filename;"

function analyzedSources(sources: ReadonlyMap<string, string>): BunPlugin {
    const files = new Map([...sources].map(([file, contents]) => [realpathSync(file), contents]))
    const loaders: Record<string, Loader> = {
        ".ts": "ts",
        ".tsx": "tsx",
        ".mts": "ts",
        ".cts": "ts",
        ".js": "js",
        ".jsx": "jsx",
        ".mjs": "js",
        ".cjs": "js",
        ".json": "json"
    }
    return {
        name: "analyzed-actor-sources",
        setup(builder) {
            builder.onLoad({ filter: /\.[cm]?[jt]sx?$|\.json$/ }, args => {
                const contents = files.get(args.path)
                const loader = loaders[path.extname(args.path)]
                return contents === undefined || loader === undefined ? undefined : { contents, loader }
            })
        }
    }
}

// ORM adapters import the actor's own ORM package, so they bundle with the actor instead of resolving from the host.
const BUNDLED_SDK_ADAPTERS = /^durable-actors\/drizzle$/

function bundledSdkAdapters(): BunPlugin {
    return {
        name: "bundled-sdk-adapters",
        setup(builder) {
            builder.onResolve({ filter: BUNDLED_SDK_ADAPTERS }, args => ({
                path: resolveFrom(args.path, args.resolveDir)
            }))
        }
    }
}

function localSdkImports(): BunPlugin {
    return {
        name: "local-sdk-imports",
        setup(builder) {
            builder.onResolve({ filter: /^durable-actors(?:\/|$)/ }, args => ({
                path: resolveFrom(args.path, args.resolveDir),
                external: true
            }))
        }
    }
}

function resolveFrom(specifier: string, directory: string): string {
    return fileURLToPath(resolve(specifier, pathToFileURL(path.join(directory, "index.js")).href))
}

// Bun's published types redefine Node's globals for the whole program, which breaks the SDK's own and its generated
// clients' type checks, so only the bundler API used here is declared.
declare const Bun: { build(config: BuildConfig): Promise<{ outputs: { text(): Promise<string> }[] }> }

interface BuildConfig {
    entrypoints: string[]
    files: Record<string, string>
    tsconfig?: string
    external: string[]
    plugins: BunPlugin[]
    loader: Record<string, Loader>
    sourcemap: "inline" | "none"
    target: "bun"
    format: "esm"
    minify: { keepNames: boolean }
    banner: string
    define: Record<string, string>
}

interface BunPlugin {
    name: string
    setup(builder: PluginBuilder): void
}

interface PluginBuilder {
    onLoad(
        options: { filter: RegExp },
        callback: (args: { path: string }) => { contents: string; loader: Loader } | undefined
    ): void
    onResolve(
        options: { filter: RegExp },
        callback: (args: { path: string; resolveDir: string }) => { path: string; external?: boolean }
    ): void
}

type Loader = "ts" | "tsx" | "js" | "jsx" | "json" | "text"

export { buildActor, bundleActor }
