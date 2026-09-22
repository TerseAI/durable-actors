import { Command, InvalidArgumentError, Option } from "commander"

import { connection, connectionOptions } from "./connection.js"
import type { ConnectionOptions } from "./connection.js"
import { ControlPlaneClient } from "./control-plane.js"

interface ActorOptions extends ConnectionOptions {
    json?: boolean
}

interface ListOptions extends ActorOptions {
    limit: number
    after?: string
    all?: boolean
}

interface SavedActor {
    actorName: string
    actorId: string
    homeRegion: string
    stateVersion: number
}

interface ActorPage {
    actors: SavedActor[]
    nextCursor: string | null
}

function registerActorCommands(program: Command): void {
    const actors = program.command("actors").description("List and inspect actors")
    connectionOptions(actors.command("list").description("List actors"))
        .addOption(
            new Option("--limit <rows>", "maximum rows to show (1–500)")
                .argParser(rowLimit)
                .default(50)
                .conflicts("all")
        )
        .addOption(new Option("--after <cursor>", "continue after the cursor from the previous page").conflicts("all"))
        .option("--all", "fetch and print every actor")
        .option("--json", "print the list as JSON")
        .action(listActors)
    connectionOptions(
        actors.command("inspect <actor-name> <actor-id>").description("Print an actor's committed state as JSON")
    ).action(inspectActor)
}

function rowLimit(value: string): number {
    const limit = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isInteger(limit) || limit < 1 || limit > 500)
        throw new InvalidArgumentError("Limit must be an integer from 1 to 500.")
    return limit
}

async function listActors(options: ListOptions): Promise<void> {
    const client = new ControlPlaneClient(connection(options), fetch)
    const actors: SavedActor[] = []
    let after: string | null = options.after ?? null
    do {
        const query = new URLSearchParams()
        query.set("limit", String(options.all ? 500 : options.limit))
        if (after) query.set("after", after)
        const page = (await client.listSavedActors(query)) as ActorPage
        actors.push(...page.actors)
        if (page.nextCursor && page.nextCursor === after) throw new Error("Server returned a repeated actor cursor.")
        after = page.nextCursor
    } while (options.all && after)
    if (options.json) console.log(JSON.stringify(actors, null, 2))
    else printActors(actors)
    if (after)
        console.error(`More actors available. Repeat this command with --after '${after.replaceAll("'", "'\\''")}'`)
}

async function inspectActor(actorName: string, actorId: string, options: ActorOptions): Promise<void> {
    const settings = connection(options)
    const client = new ControlPlaneClient(settings, fetch)
    const result = await client.inspectActor(actorName, actorId)
    console.log(JSON.stringify(result, null, 2))
}

function printActors(actors: SavedActor[]): void {
    if (!actors.length) {
        console.log("No actors found.")
        return
    }
    const rows = [
        ["TYPE", "ID", "VERSION", "REGION"],
        ...actors.map(actor => [actor.actorName, actor.actorId, String(actor.stateVersion), actor.homeRegion])
    ]
    const widths = rows[0]!.map((_, column) =>
        rows.reduce((width, row) => Math.max(width, (row[column] ?? "").length), 0)
    )
    for (const row of rows)
        console.log(
            row
                .map((value, column) => (value ?? "").padEnd(widths[column]!))
                .join("  ")
                .trimEnd()
        )
}

export { registerActorCommands }
