import { readSync, writeSync } from "node:fs"

/** A connection whose reads and writes hold the calling thread until they are done. */
interface BlockingStream {
    /** Reads at least one byte into `into`, starting at `offset`. Returns 0 once the peer has closed. */
    read(into: Uint8Array, offset: number): number
    write(bytes: Uint8Array): void
}

class DescriptorStream implements BlockingStream {
    constructor(private readonly descriptor: number) {}

    read(into: Uint8Array, offset: number): number {
        return readSync(this.descriptor, into, offset, into.byteLength - offset, null)
    }

    write(bytes: Uint8Array): void {
        for (let written = 0; written < bytes.byteLength;)
            written += writeSync(this.descriptor, bytes, written, bytes.byteLength - written)
    }
}

export { DescriptorStream }
export type { BlockingStream }
