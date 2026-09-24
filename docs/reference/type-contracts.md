# Type preservation in generated clients

The compiler publishes actor types as JSON Schema in the public contract. Client generation reads that contract without importing or executing the actor or its dependencies.

String dictionaries, including mapped types such as `Record<string, JSONObject>`, use `additionalProperties` with a value schema. Dictionaries retain their value types inside unions, intersections, arrays and recursive structures.

`unknown` is supported in RPC parameters and results, socket metadata and messages, and public persisted state, including nested properties and container values. It becomes an unconstrained JSON Schema and stays `unknown` in generated declarations. Application code must narrow returned values before using them, and runtime values still need to be JSON-compatible.

Template-literal types with `string`, `number`, and `bigint` substitutions retain their patterns in generated declarations. Literal unions expanded by TypeScript remain unions. Unsupported template substitutions produce a compiler error instead of silently becoming `string`.

Optional properties and dictionary values retain `undefined` in their TypeScript declarations. JSON serialization still omits object entries whose value is `undefined`; the annotation does not change runtime serialization. Standalone `undefined` RPC values and `undefined` array elements remain unsupported because JSON cannot represent them without changing their meaning.

## Additional TypeScript information

Two validated fields under `x-typescript` carry distinctions that ordinary JSON Schema does not preserve for TypeScript generation:

```json
{
  "type": "string",
  "x-typescript": {
    "templateLiteral": {
      "texts": ["", ".", ""],
      "types": ["string", "string"]
    }
  }
}
```

This emits the TypeScript type `` `${string}.${string}` ``. An `undefined: true` marker adds `undefined` to the generated value type. These fields contain structured data rather than arbitrary TypeScript source. The generator validates them and uses the TypeScript AST printer to escape literal text. JSON Schema validators can ignore the annotations; strict Ajv configurations should register `x-typescript` as an annotation keyword.

The AI SDK `UIMessage` regression compiles actor source, serializes its public contract, deletes the actor source, generates the client, and then compiles this application code with `strict: true` and `skipLibCheck: false`:

```ts
await chat.append(message) // message: UIMessage
const messages: UIMessage[] = await chat.load()
await convertToModelMessages(messages)
```

To repair a client generated from a contract that lost type information, rebuild and publish the actor contract with the updated compiler, then regenerate the application client. Regenerating from an old contract cannot recover information absent from that contract.
