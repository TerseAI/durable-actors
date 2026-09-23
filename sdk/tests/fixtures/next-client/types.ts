import type { actors } from "./generated/index.js"

const args: actors.ChatRoom.Methods.sendMessage.Args = [{ text: "hello" }]
const result: actors.ChatRoom.Methods.sendMessage.Result = { id: "1", text: "hello" }
// @ts-expect-error method arguments retain their types
const invalidArgs: actors.ChatRoom.Methods.sendMessage.Args = [{ text: 42 }]
// @ts-expect-error method results retain their types
const invalidResult: actors.ChatRoom.Methods.sendMessage.Result = { id: 42, text: "hello" }
