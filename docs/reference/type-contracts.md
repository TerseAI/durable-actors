# Type preservation in generated clients

The compiler publishes actor types as JSON Schema in the public contract. Client generation reads that contract without importing or executing the actor or its dependencies.

String dictionaries, including mapped types such as `Record<string, JSONObject>`, use `additionalProperties` with a value schema. Dictionaries retain their value types inside unions, intersections, arrays and recursive structures.

`unknown` is supported in RPC parameters and results, socket metadata and messages, and public persisted state, including nested properties and container values. It becomes an unconstrained JSON Schema and stays `unknown` in generated declarations, so application code must narrow returned values before using them. Runtime values still need to be JSON-compatible.

For a type that restricts values to JSON at compile time, use an explicit recursive type:

```ts
type Json = null | boolean | number | string | Json[] | { [key: string]: Json }
```

The contract uses standard JSON Schema. Generated declarations do not preserve every TypeScript distinction: template-literal types become `string`, and `undefined` is lost from dictionary value types. Optional properties remain optional. An unmodified AI SDK `UIMessage` still does not round-trip with full type compatibility because of these unresolved type features. Applications must define a supported storage type for the message shapes they use.

To repair a client generated from a contract that lost type information, rebuild and publish the actor contract with the updated compiler, then regenerate the application client. Regenerating from an old contract cannot recover information absent from that contract.
