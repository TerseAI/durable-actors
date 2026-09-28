import { closeSync, ftruncateSync, mkdtempSync, openSync, rmSync, writeSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

import type { SqliteState } from "../../src/host/sqlite.js"

export class SqliteRecovery {
    private readonly directory = mkdtempSync(join(tmpdir(), "sqlite-recovery-test-"))
    private readonly path = join(this.directory, "actor.sqlite")
    private readonly file = openSync(this.path, "w+")

    apply(state: SqliteState | undefined): SqliteState | undefined {
        if (state === undefined) return undefined
        if (state.wal !== undefined) {
            const wal = Buffer.from(state.wal.data, "base64")
            const pageSize = wal.readUInt32BE(8)
            for (let offset = 32; offset + 24 + pageSize <= wal.length; offset += 24 + pageSize) {
                const page = wal.readUInt32BE(offset)
                writeSync(
                    this.file,
                    wal.subarray(offset + 24, offset + 24 + pageSize),
                    0,
                    pageSize,
                    (page - 1) * pageSize
                )
                const commit = wal.readUInt32BE(offset + 4)
                if (commit !== 0) ftruncateSync(this.file, commit * pageSize)
            }
        }
        return { txid: state.txid, path: this.path }
    }

    close(): void {
        closeSync(this.file)
        rmSync(this.directory, { recursive: true, force: true })
    }
}
