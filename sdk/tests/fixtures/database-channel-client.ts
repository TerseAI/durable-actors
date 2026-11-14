import { errorMessage } from "../../src/errors.js"
import { FileDescriptorDatabaseChannel } from "../../src/host/database-channel.js"

const channel = new FileDescriptorDatabaseChannel(3)
const outcomes: unknown[] = []
for (const messageId of [7, undefined, 8]) {
    try {
        const result = channel.call({ op: "fields" }, messageId)
        outcomes.push(result.type === "fields" ? { size: String(result.fields.big).length } : result)
    } catch (error) {
        outcomes.push({ error: errorMessage(error) })
    }
}
console.log(JSON.stringify(outcomes))
