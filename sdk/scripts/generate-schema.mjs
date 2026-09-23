import { writeFile } from "node:fs/promises"
import { fileURLToPath } from "node:url"
import protobuf from "protobufjs"

const root = protobuf.loadSync(fileURLToPath(new URL("../../proto/durable_actors.proto", import.meta.url)))
root.resolveAll()
const source = `import type { fromJSON } from "@grpc/proto-loader"
export default ${JSON.stringify(root.toJSON())} as Parameters<typeof fromJSON>[0]
`
await writeFile(new URL("../src/generated/schema.ts", import.meta.url), source)
