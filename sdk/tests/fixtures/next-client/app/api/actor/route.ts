import { type ActorRpcTransport, actors } from "../../../generated/index.js"

export const dynamic = "force-dynamic"

const transport: ActorRpcTransport = {
    async invoke(actorName, actorId, method, args) {
        return { id: `${actorName}/${actorId}/${method}`, text: (args[0] as { text: string }).text }
    }
}

export async function GET() {
    const room = actors.ChatRoom.get("lobby", transport)
    const args: actors.ChatRoom.Methods.sendMessage.Args = [{ text: "hello" }]
    const message: actors.ChatRoom.Methods.sendMessage.Result = await room.sendMessage(...args)
    return Response.json(message)
}
