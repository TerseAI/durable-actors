import { z } from "zod"

import { ActorProtocolError } from "../errors.js"

const NEWLINE = 0x0a

/** One session message: a line of JSON, then the parts whose sizes the line announces in `parts`. */
interface SessionFrame {
    readonly head: unknown
    readonly parts: readonly Uint8Array[]
}

// Each chunk is scanned once. A part is copied into a buffer of its own, so it is never searched or held as text.
class SessionFrames {
    private readonly line: Buffer[] = []
    private filling: FillingFrame | undefined

    accept(chunk: Buffer): SessionFrame[] {
        const frames: SessionFrame[] = []
        for (let rest = chunk; rest.length > 0;) {
            rest = this.filling === undefined ? this.readLine(rest) : this.filling.fill(rest)
            if (this.filling?.isComplete()) frames.push(this.takeFrame())
        }
        return frames
    }

    private readLine(chunk: Buffer): Buffer {
        const end = chunk.indexOf(NEWLINE)
        if (end === -1) {
            this.line.push(chunk)
            return chunk.subarray(chunk.length)
        }
        const head = parseHead(this.completeLine(chunk.subarray(0, end)))
        this.filling = new FillingFrame(head, partSizes(head))
        return chunk.subarray(end + 1)
    }

    private completeLine(end: Buffer): Buffer {
        return this.line.length === 0 ? end : Buffer.concat([...this.line.splice(0), end])
    }

    private takeFrame(): SessionFrame {
        const frame = this.filling!
        this.filling = undefined
        return frame
    }
}

class FillingFrame implements SessionFrame {
    readonly parts: Uint8Array[]
    private part = 0
    private filled = 0

    constructor(
        readonly head: unknown,
        sizes: readonly number[]
    ) {
        this.parts = sizes.map(size => new Uint8Array(size))
        this.skipFilledParts()
    }

    isComplete(): boolean {
        return this.part === this.parts.length
    }

    fill(chunk: Buffer): Buffer {
        const part = this.parts[this.part]!
        const taken = chunk.subarray(0, part.length - this.filled)
        part.set(taken, this.filled)
        this.filled += taken.length
        this.skipFilledParts()
        return chunk.subarray(taken.length)
    }

    private skipFilledParts(): void {
        while (!this.isComplete() && this.filled === this.parts[this.part]!.length) {
            this.part++
            this.filled = 0
        }
    }
}

function parseHead(line: Buffer): unknown {
    try {
        return JSON.parse(line.toString())
    } catch (error) {
        throw new ActorProtocolError("actor session message is not valid JSON", { cause: error })
    }
}

function partSizes(head: unknown): readonly number[] {
    const result = partSizesSchema.safeParse(head)
    if (!result.success) throw new ActorProtocolError(`actor session message is invalid: ${result.error.message}`)
    return result.data.parts
}

const partSizesSchema = z.object({ parts: z.array(z.number().int().nonnegative()).default([]) })

export { SessionFrames }
export type { SessionFrame }
