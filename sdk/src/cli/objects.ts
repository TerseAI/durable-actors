import { Command, InvalidArgumentError, Option } from "commander"

import { connection, connectionOptions } from "./connection.js"
import type { ConnectionOptions } from "./connection.js"
import { ControlPlaneClient } from "./control-plane.js"

interface ObjectOptions extends ConnectionOptions {
    json?: boolean
}

interface ListOptions extends ObjectOptions {
    limit: number
    after?: string
    all?: boolean
}

interface SavedObject {
    namespaceId: string
    actorType: string
    actorId: string
    homeRegion: string
    stateVersion: number
}

interface ObjectPage {
    objects: SavedObject[]
    nextCursor: string | null
}

function registerObjectCommands(program: Command): void {
    const objects = program.command("objects").description("List saved actors and inspect committed internal state")
    connectionOptions(objects.command("list").description("List saved objects across all namespaces"))
        .addOption(
            new Option("--limit <rows>", "maximum rows to show (1–500)")
                .argParser(rowLimit)
                .default(50)
                .conflicts("all")
        )
        .addOption(new Option("--after <cursor>", "continue after the cursor from the previous page").conflicts("all"))
        .option("--all", "fetch and print every saved object")
        .option("--json", "print the list as JSON")
        .action(listObjects)
    connectionOptions(
        objects.command("inspect <actor-type> <actor-id>").description("Print an object's committed state as JSON")
    ).action(inspectObject)
}

function rowLimit(value: string): number {
    const limit = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isInteger(limit) || limit < 1 || limit > 500)
        throw new InvalidArgumentError("Limit must be an integer from 1 to 500.")
    return limit
}

async function listObjects(options: ListOptions): Promise<void> {
    const client = new ControlPlaneClient(connection(options), fetch)
    const objects: SavedObject[] = []
    let after: string | null = options.after ?? null
    do {
        const query = new URLSearchParams()
        if (options.namespace) query.set("namespace", options.namespace)
        query.set("limit", String(options.all ? 500 : options.limit))
        if (after) query.set("after", after)
        const page = (await client.listObjects(query)) as ObjectPage
        objects.push(...page.objects)
        if (page.nextCursor && page.nextCursor === after) throw new Error("Server returned a repeated object cursor.")
        after = page.nextCursor
    } while (options.all && after)
    if (options.json) console.log(JSON.stringify(objects, null, 2))
    else printObjects(objects)
    if (after)
        console.error(`More objects available. Repeat this command with --after '${after.replaceAll("'", "'\\''")}'`)
}

async function inspectObject(actorType: string, actorId: string, options: ObjectOptions): Promise<void> {
    const settings = connection(options)
    const client = new ControlPlaneClient(settings, fetch)
    const result = await client.inspectObject(actorType, actorId)
    console.log(JSON.stringify(result, null, 2))
}

function printObjects(objects: SavedObject[]): void {
    if (!objects.length) {
        console.log("No saved objects found.")
        return
    }
    const rows = [
        ["NAMESPACE", "TYPE", "ID", "VERSION", "REGION"],
        ...objects.map(object => [
            object.namespaceId,
            object.actorType,
            object.actorId,
            String(object.stateVersion),
            object.homeRegion
        ])
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

export { registerObjectCommands }
