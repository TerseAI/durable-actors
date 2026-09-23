# Type preservation in generated clients

The compiler publishes actor types as JSON Schema in the public contract. Client generation reads that contract without importing or executing the actor or its dependencies.

String dictionaries, including mapped types such as `Record<string, JSONObject>`, use `additionalProperties` with a value schema. Unconstrained schemas generate `unknown`. Union members retain their own property, array and dictionary types.

The contract uses standard JSON Schema. Generated declarations do not preserve every TypeScript distinction: template-literal types become `string`, and `undefined` is lost from dictionary value types. Optional properties remain optional. The full AI SDK `UIMessage` append/load/`convertToModelMessages` integration still requires those unresolved type features.

For a storage boundary, applications can keep the message envelope typed and validate the parts after loading:

```ts
import type { UIMessage } from "ai"

type StoredMessage = Omit<UIMessage, "parts"> & { parts: unknown[] }
```

Use `StoredMessage` for the actor's `append` parameter and persisted messages, and declare `load(): Promise<StoredMessage[]>`. Then the application can use:

```ts
import { convertToModelMessages, validateUIMessages } from "ai"

await chat.append(message) // message: UIMessage
const messages = await validateUIMessages({ messages: await chat.load() })
await convertToModelMessages(messages)
```

This keeps message contents while deferring the type of `parts` to runtime validation. Supply the corresponding tool definitions and metadata/data schemas to [`validateUIMessages`](https://ai-sdk.dev/docs/reference/ai-sdk-core/validate-ui-messages) when those custom structures need validation.

To repair a client generated from a contract that lost type information, rebuild and publish the actor contract with the updated compiler, then regenerate the application client. Regenerating from an old contract cannot recover information absent from that contract.
