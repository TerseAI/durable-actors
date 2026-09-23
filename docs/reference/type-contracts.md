# Type preservation in generated clients

The compiler publishes actor types as JSON Schema in the public contract. Client generation reads that contract without importing or executing the actor or its dependencies.

String dictionaries, including mapped types such as `Record<string, JSONObject>`, use `additionalProperties` with a value schema. Unconstrained schemas generate `unknown`. Union members retain their own property, array and dictionary types.

Details that JSON cannot represent use an optional `x-typescript` annotation on the relevant schema node:

```json
{
    "type": "string",
    "pattern": "^.*\\..*$",
    "x-typescript": {
        "templateLiteral": {
            "texts": ["", ".", ""],
            "types": ["string", "string"]
        }
    }
}
```

This generates the TypeScript type `${string}.${string}`. Template spans support `string`, `number` and `bigint`; literal text is escaped during generation. The `texts` array has one more entry than `types`.

An annotation of `{"undefined": true}` adds `undefined` to the generated type. For example, a dictionary value can retain `Json | undefined` while its JSON Schema describes only the serialized JSON values. The existing restrictions on top-level `undefined` and undefined array elements still apply.

These annotations contain structured data, never TypeScript source or dependency imports. They do not change JSON validation. Strict schema validators must recognize the annotation keyword; with Ajv, use `ajv.addKeyword("x-typescript")`.

To repair a client generated from a contract that lost type information, rebuild and publish the actor contract with the updated compiler, then regenerate the application client. Regenerating from an old contract cannot recover information absent from that contract.
