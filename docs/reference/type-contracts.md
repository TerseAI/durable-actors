# Types in generated clients

Each deployment publishes JSON Schemas for runtime validation and standard TypeScript declarations for the public actor API. The declarations are emitted from the actor's TypeScript by the TypeScript compiler. `dts-bundle-generator` bundles local supporting types and preserves imports of third-party types.

The published contract includes:

```json
{
  "version": 1,
  "actors": [],
  "typescript": {
    "declarations": "export interface ActorTypes {}",
    "dependencies": {}
  }
}
```

The contract hash covers the schemas, declarations, and dependency versions together. The control plane stores and serves them as one deployment artifact. Server-based `generate` downloads this contract and writes `index.js`, `index.d.ts`, `types.d.ts`, `package.json`, and the standalone runtime. The calling project does not need the actor source or the durable-actors SDK at runtime.

## External type dependencies

When an actor uses `UIMessage` from `ai`, the declarations retain an import from `ai`. Its build version is recorded in `typescript.dependencies` and in the generated package's `peerDependencies`. The CLI reports the required type packages; install compatible versions in the calling project. Generation itself does not need to install or load these packages. JavaScript consumers do not load type-only dependencies at runtime.

The AI SDK regression compiles the actor, serializes its public contract, removes the actor source, generates the client, and checks the following application code with `strict: true` and `skipLibCheck: false`:

```ts
await chat.append(message) // message: UIMessage
const messages: UIMessage[] = await chat.load()
await convertToModelMessages(messages)
```

Local dictionaries, readonly arrays, recursive types, unions, intersections, unknown values, template literals, and optional undefined values retain their TypeScript declarations. Client types are no longer reconstructed from JSON Schema, and no custom TypeScript annotations are needed inside schemas.

Public types are available through `actors.Room.Metadata`, `Incoming`, `Outgoing`, `State`, and `Methods`. Access method argument and result types through `actors.Room.Methods["send"]["Args"]` and `actors.Room.Methods["send"]["Result"]`; supporting declaration names are implementation details.

## Runtime limits

The JSON Schema compiler and JSON transport still determine which actor values are supported. Standard declarations do not make functions, classes, symbols, or arbitrary non-JSON values serializable. Optional tuple elements and some constrained dictionary keys remain outside the current schema compiler's supported subset even though TypeScript can represent them.

JSON serialization omits object entries whose value is `undefined`. Standalone undefined RPC values and undefined array elements remain unsupported. An `unknown` declaration requires narrowing in application code, and values sent at runtime must still be JSON-compatible.

Rebuild the actor deployment and regenerate its clients to use the declaration-based contract format. Existing contracts without declarations must be rebuilt.
