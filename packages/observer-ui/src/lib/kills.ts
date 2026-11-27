import type { KillReason } from "../types/Kills.js"

export const killLabels: Record<KillReason, string> = {
    out_of_memory: "Out of memory",
    out_of_disk: "Out of disk",
    evicted: "Evicted",
    killed: "Killed"
}
