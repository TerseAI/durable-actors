import { build } from "esbuild"
import { readFile } from "node:fs/promises"
import { fileURLToPath } from "node:url"
import protobuf from "protobufjs"

let artifacts: Promise<ReadonlyMap<string, string>> | undefined

export function runtimeArtifacts(): Promise<ReadonlyMap<string, string>> {
    artifacts ??= bundleRuntime().catch(error => {
        artifacts = undefined
        throw error
    })
    return artifacts
}

async function bundleRuntime(): Promise<ReadonlyMap<string, string>> {
    const root = new URL("../../", import.meta.url)
    const protocol = protobuf.loadSync(fileURLToPath(new URL("generated/durable_actors.proto", root))).toJSON()
    const result = await build({
        entryPoints: [fileURLToPath(new URL("generated-runtime/index.js", root))],
        bundle: true,
        platform: "node",
        format: "esm",
        target: "node20",
        minify: true,
        legalComments: "eof",
        write: false,
        banner: { js: 'import { createRequire } from "node:module"; const require = createRequire(import.meta.url);' },
        define: { "process.env.WS_NO_BUFFER_UTIL": '"1"', "process.env.WS_NO_UTF_8_VALIDATE": '"1"' },
        plugins: [
            {
                name: "embedded-actor-protocol",
                setup(build) {
                    build.onLoad({ filter: /[/\\]actorHostDefinition\.js$/ }, () => ({
                        contents: `import { loadPackageDefinition } from "@grpc/grpc-js"
                        import { fromJSON } from "@grpc/proto-loader"
                        export const ActorHostClient = loadPackageDefinition(fromJSON(${JSON.stringify(protocol)}, { defaults: true, longs: Number, oneofs: true })).durable_actors.v1.ActorHostService`,
                        resolveDir: fileURLToPath(new URL("client/", root)),
                        loader: "js"
                    }))
                }
            }
        ]
    })
    const [types, declarations, browser] = await Promise.all([
        readFile(new URL("generated-runtime/types.d.ts", root), "utf8"),
        readFile(new URL("generated-runtime/index.d.ts", root), "utf8"),
        readFile(new URL("generated.browser.js", root), "utf8")
    ])
    return new Map([
        ["runtime/index.js", result.outputFiles[0]!.text],
        ["runtime/index.d.ts", withoutSourceMap(declarations)],
        ["runtime/types.d.ts", withoutSourceMap(types)],
        ["runtime/browser.js", withoutSourceMap(browser)],
        [
            "runtime/package.json",
            JSON.stringify({ type: "module", sideEffects: false, browser: { "./index.js": "./browser.js" } }, null, 4) +
                "\n"
        ]
    ])
}

function withoutSourceMap(source: string): string {
    return source.replace(/^\/\/# sourceMappingURL=.*$/gm, "")
}
