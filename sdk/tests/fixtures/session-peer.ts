import type { Socket } from "node:net"

import { SessionFrames } from "../../src/payload/session-frames.js"
import type { SessionFrame } from "../../src/payload/session-frames.js"

import { attachEffects, attachReply } from "./detached.js"
import { withResult } from "./reply.js"

/** The Rust host's end of an actor session: messages are read and written whole, with their parts in place. */
export function readSession(socket: Socket, handle: (message: any) => void): void {
    const frames = new SessionFrames()
    socket.on("data", (chunk: Buffer) => {
        for (const frame of frames.accept(chunk)) handle(attach(frame))
    })
}

export function sessionReader(socket: Socket): () => Promise<any> {
    const messages: unknown[] = []
    const waiting: ((message: unknown) => void)[] = []
    readSession(socket, message => {
        const next = waiting.shift()
        if (next === undefined) messages.push(message)
        else next(message)
    })
    return () =>
        new Promise(resolve => {
            if (messages.length > 0) resolve(messages.shift())
            else waiting.push(resolve)
        })
}

export function writeSession(socket: Socket, message: object): void {
    socket.write(`${JSON.stringify(message)}\n`)
}

function attach(frame: SessionFrame): unknown {
    const { parts: _sizes, ...message } = frame.head as { type: string; parts?: number[]; reply?: object }
    const parts = frame.parts.map(part => Buffer.from(part).toString())
    if (message.type === "socket_effects") return { ...message, effects: attachEffects(parts) }
    if (message.type !== "reply") return message
    return { ...message, reply: withResult(attachReply({ ...message.reply, parts } as never)) }
}
