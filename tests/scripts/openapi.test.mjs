import SwaggerParser from "@apidevtools/swagger-parser"
import Ajv from "ajv/dist/2020.js"
import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

const specPath = fileURLToPath(new URL("../../docs/reference/openapi.yaml", import.meta.url))

test("OpenAPI validates and covers the public HTTP routes", async () => {
    const spec = await SwaggerParser.validate(specPath)
    const paths = new Set()
    for (const file of ["control_plane/public_api.rs", "control_plane/contract_api.rs", "control_plane/inspection.rs", "sockets/browser.rs"]) {
        const source = await readFile(new URL(`../../src/${file}`, import.meta.url), "utf8")
        for (const match of source.matchAll(/\.route\(\s*"([^"]+)"/gu)) paths.add(match[1])
    }
    assert.deepEqual(Object.keys(spec.paths).sort(), [...paths].sort())
    const operations = Object.values(spec.paths).flatMap(path => ["get", "post", "put", "delete"].filter(method => path[method]).map(method => path[method]))
    assert.equal(new Set(operations.map(operation => operation.operationId)).size, operations.length)
    assert.ok(operations.every(operation => operation.operationId))
})

test("OpenAPI examples satisfy their request and response schemas", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const ajv = new Ajv({ strict: false, validateFormats: false })
    for (const paths of [spec.paths, spec.webhooks]) {
        for (const path of Object.values(paths)) {
            for (const method of ["get", "post", "put", "delete"]) {
                const operation = path[method]
                if (!operation) continue
                for (const body of [operation.requestBody, ...Object.values(operation.responses)]) {
                    for (const media of Object.values(body?.content ?? {})) {
                        for (const example of Object.values(media.examples ?? {})) {
                            assert.ok(ajv.validate(media.schema, example.value), `${operation.operationId}: ${ajv.errorsText()}`)
                        }
                    }
                }
            }
        }
    }
})

test("connection schemas distinguish transports and enforce grant limits", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const validate = new Ajv({ strict: false }).compile(spec.components.schemas.ConnectRequest)
    assert.ok(validate({ transport: "grpc" }))
    assert.ok(validate({ transport: "websocket", metadata: null }))
    for (const request of [
        { transport: "grpc", metadata: {} },
        { transport: "websocket" },
        { transport: "websocket", metadata: {}, authorizationLifetimeMs: 999 },
        { transport: "websocket", metadata: {}, authorizationLifetimeMs: 86400001 },
        { transport: "http" }
    ])
        assert.equal(validate(request), false, JSON.stringify(request))
})

test("OpenAPI accepts the compiler's public contract fixture", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const contract = JSON.parse(await readFile(new URL("../../sdk/tests/fixtures/public-contract.json", import.meta.url), "utf8"))
    const ajv = new Ajv({ strict: false })
    assert.ok(ajv.validate(spec.components.schemas.PublicActorContract, contract), ajv.errorsText())
})
