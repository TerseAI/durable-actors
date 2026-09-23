import type { actors } from "../generated/index.js"

export default function Page() {
    const args: actors.ChatRoom.Methods.sendMessage.Args = [{ text: "Standalone client" }]
    return <main>{args[0].text}</main>
}
