import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import ts from "typescript"
import { z } from "zod"

import { generateTypeScript } from "../../src/compiler/generators/typescript-generator.js"
import { parsePublicContract } from "../../src/compiler/validate-public-contract.js"

const fixture = JSON.parse(
    await readFile(new URL("../../../tests/fixtures/public-contract.json", import.meta.url), "utf8")
)

test("contract parsing reports invalid embedded schemas at their contract paths", () => {
    for (const kind of ["socket", "rpc"]) {
        const document = structuredClone(fixture)
        document.actors[0][kind].schema.definitions.Invalid = { type: "invalid" }
        assert.throws(
            () => parsePublicContract(document),
            error => {
                assert.ok(error instanceof z.ZodError)
                assert.deepEqual(error.issues[0].path, ["actors", 0, kind, "schema"])
                assert.match(error.issues[0].message, /invalid contract schema/)
                return true
            }
        )
    }
})

test("codegen rejects malformed contracts and unsafe type overrides", async () => {
    const cases: [string, (document: typeof fixture) => void, RegExp][] = [
        ["duplicate actors", document => document.actors.push(document.actors[0]), /duplicate actor/],
        ["path traversal", document => (document.actors[0].actorName = "../outside"), /Invalid/],
        ["keyword actor", document => (document.actors[0].actorName = "class"), /identifier/],
        ["mismatched socket", document => (document.actors[0].socket.actorName = "Other"), /must match/],
        [
            "missing type",
            document => delete document.actors[0].socket.schema.definitions.Incoming,
            /reference is missing/
        ],
        [
            "duplicate methods",
            document => document.actors[0].rpc.methods.push(document.actors[0].rpc.methods[0]),
            /duplicate RPC/
        ],
        ["reserved method", document => (document.actors[0].rpc.methods[0].name = "then"), /reserved RPC/],
        [
            "missing result type",
            document => (document.actors[0].rpc.methods[1].result.type.$ref = "#/definitions/Absent"),
            /reference is missing/
        ],
        [
            "external type",
            document =>
                (document.actors[0].rpc.schema.definitions.Method_sendMessage_Result = {
                    $ref: "https://example.com/types.json"
                }),
            /local definitions/
        ],
        [
            "raw TypeScript",
            document => (document.actors[0].rpc.schema.definitions.Method_sendMessage_Result = { tsType: "any" }),
            /override type resolution/
        ],
        [
            "nested raw TypeScript",
            document => (document.actors[0].socket.schema.definitions.Incoming.properties.text = { tsType: "any" }),
            /override type resolution/
        ],
        [
            "scope override",
            document => (document.actors[0].rpc.schema.$id = "file:///tmp/"),
            /override type resolution/
        ],
        [
            "invalid schema",
            document => (document.actors[0].rpc.schema.definitions.Method_sendMessage_Result.type = "invalid"),
            /invalid contract schema/
        ],
        [
            "non-array rest",
            document => (document.actors[0].rpc.methods[1].parameters[0].rest = true),
            /array parameter/
        ],
        ["unknown state field", document => document.actors[0].socket.emittable.push("secret"), /not public state/]
    ]
    for (const [label, mutate, expected] of cases) {
        const document = structuredClone(fixture)
        mutate(document)
        await assert.rejects(generateTypeScript(document), expected, label)
    }
})

test("contract validation permits schema-like property names and recursive local definitions", async () => {
    const document = structuredClone(fixture)
    document.actors[0].rpc.schema.definitions.Method_sendMessage_Result = {
        type: "object",
        description: "*/\nexport const injected = 1;\n/**",
        properties: {
            tsType: { type: "string" },
            $ref: { type: "string" },
            next: { $ref: "#/definitions/Method_sendMessage_Result" }
        }
    }
    assert.deepEqual(parsePublicContract(document), document)
    const files = await generateTypeScript(document)
    const code = files.get("index.d.ts")!
    assert.match(code, /next\?: SendMessageResult/)
    const source = ts.createSourceFile("backend.ts", code, ts.ScriptTarget.Latest, true)
    const variables = source.statements
        .filter(ts.isVariableStatement)
        .flatMap(statement =>
            statement.declarationList.declarations.map(declaration => declaration.name.getText(source))
        )
    assert.deepEqual(variables, ["actors"])
})
