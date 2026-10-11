import { resolve } from "node:path"
import { fileURLToPath } from "node:url"

export function ticketFromBranch(branch) {
    const tickets = [...(branch ?? "").matchAll(/(?:^|[/-])(DUR-[1-9]\d*)(?=$|[/-])/gi)]
    if (tickets.length !== 1) throw new Error("The source branch must contain exactly one DUR ticket, for example codex/DUR-123-fix-reconnect. Ask a maintainer for the ticket ID if needed.")
    return tickets[0][1].toUpperCase()
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        console.log(`Ticket identifier: ${ticketFromBranch(process.env.PR_HEAD_REF)}`)
    } catch (error) {
        console.error(error.message)
        process.exitCode = 1
    }
}
