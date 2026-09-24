import type { TypeScriptContract } from "../../src/wire/public-contract.js"

export function declarations(actors: Record<string, string>): TypeScriptContract {
    return {
        declarations: `export interface ActorTypes { ${Object.entries(actors)
            .map(([name, body]) => `${JSON.stringify(name)}: { ${body} }`)
            .join("; ")} }`,
        dependencies: {}
    }
}
