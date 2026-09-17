import { build } from "esbuild"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"

import { ACTOR_ARTIFACT_VERSION } from "../actor/schema.js"

import { ActorCompiler } from "./actor-compiler.js"
import type { CompilerOptions } from "./types.js"

async function buildActor(entrypoint: string, outfile: string, options: CompilerOptions = {}): Promise<void> {
    if (!outfile.endsWith(".mjs")) throw new Error("actor build output must use the .mjs extension")
    const source = path.resolve(entrypoint)
    const schemas = new ActorCompiler().compile(source, options)
    const result = await build({
        stdin: {
            contents: `import * as actors from ${JSON.stringify(source)};
                export { actors };
                export const schemas = ${JSON.stringify(schemas)};
                export const version = ${ACTOR_ARTIFACT_VERSION};`,
            resolveDir: path.dirname(source),
            sourcefile: "actor-artifact.ts",
            loader: "ts"
        },
        outfile,
        tsconfig: options.configFile,
        bundle: true,
        packages: "external",
        platform: "node",
        format: "esm",
        target: "node20",
        keepNames: true,
        write: false,
        logLevel: "silent"
    })
    await mkdir(path.dirname(outfile), { recursive: true })
    await writeFile(outfile, result.outputFiles[0]!.contents)
}

export { buildActor }
