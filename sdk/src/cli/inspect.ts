import { Command } from "commander"
import { z } from "zod"

import { validateActorComponent } from "../actor/identity.js"

import { connectionHelp } from "./connection.js"
import { ControlPlaneClient, createControlPlaneClient } from "./control-plane.js"

interface InspectOptions {
    json?: boolean
    controlPlaneUrl?: string
}

interface ActorSelector {
    actorName: string
    actorId: string
}

const waitingSchema = z.object({ id: z.string(), operation: z.string() })
const connectionSchema = z.object({ id: z.string(), metadata: z.unknown() })
const instanceSchema = z.object({
    actorId: z.string(),
    status: z.enum(["live", "dormant", "unknown"]),
    connections: z.array(connectionSchema),
    waiting: z.array(waitingSchema).nullable().optional()
})
const inventorySchema = z.object({
    connectionsComplete: z.boolean(),
    actors: z.array(
        z.object({
            actorName: z.string(),
            live: z.number().int().nonnegative(),
            dormant: z.number().int().nonnegative(),
            unknown: z.number().int().nonnegative(),
            instances: z.array(instanceSchema)
        })
    )
})
const stateSchema = z.object({
    snapshot: z
        .looseObject({
            stateVersion: z.number(),
            ownerEpoch: z.number(),
            requestId: z.string(),
            state: z.record(z.string(), z.unknown())
        })
        .nullable(),
    schema: z.record(z.string(), z.unknown()).nullable()
})

type Inventory = z.infer<typeof inventorySchema>
type ActorInstance = z.infer<typeof instanceSchema>
type StateResponse = z.infer<typeof stateSchema>

function registerInspectCommand(program: Command): void {
    program
        .command("inspect")
        .argument("[selector]", "actor instance as ActorName/actorId")
        .description("Print actor inventory and persisted snapshots from the control plane")
        .option("--json", "print machine-readable JSON")
        .option("--control-plane-url <url>", "control-plane origin (overrides DURABLE_ACTORS_CONTROL_PLANE_URL)")
        .addHelpText("after", connectionHelp)
        .action(inspect)
}

async function inspect(selector: string | undefined, options: InspectOptions): Promise<void> {
    const client = createControlPlaneClient(
        {
            ...process.env,
            DURABLE_ACTORS_CONTROL_PLANE_URL: options.controlPlaneUrl ?? process.env.DURABLE_ACTORS_CONTROL_PLANE_URL
        },
        fetch
    )
    const result = await inspectActors(client, { selector, json: options.json })
    if (result.warning) console.error(result.warning)
    console.log(result.text)
}

async function inspectActors(
    client: Pick<ControlPlaneClient, "listActors" | "getState">,
    options: { selector?: string; json?: boolean } = {}
): Promise<{ text: string; warning?: string }> {
    const inventory = readInventory(await client.listActors())
    const warning = inventory.connectionsComplete ? undefined : "Warning: connection inventory is incomplete."
    if (!options.selector) return { text: options.json ? stringify(inventory) : formatInventory(inventory), warning }
    const selector = parseActorSelector(options.selector)
    const instance = findInstance(inventory, selector)
    if (!instance) throw new Error(`No actor instance ${selector.actorName}/${selector.actorId}.`)
    const state = readState(
        await client.getState(new URLSearchParams({ actorName: selector.actorName, actorId: selector.actorId }))
    )
    return {
        text: options.json
            ? stringify({ actorName: selector.actorName, actorId: selector.actorId, instance, state })
            : formatInstance(selector, instance, state),
        warning
    }
}

function parseActorSelector(value: string): ActorSelector {
    const separator = value.indexOf("/")
    if (separator <= 0 || separator !== value.lastIndexOf("/") || separator === value.length - 1)
        throw new Error("Expected ActorName/actorId.")
    try {
        return {
            actorName: validateActorComponent("actorName", value.slice(0, separator)),
            actorId: validateActorComponent("actorId", value.slice(separator + 1))
        }
    } catch {
        throw new Error("Expected ActorName/actorId.")
    }
}

function formatInventory(inventory: Inventory): string {
    if (inventory.actors.length === 0) return "No actor instances."
    return inventory.actors.map(formatClass).join("\n")
}

function formatInstance(selector: ActorSelector, instance: ActorInstance, state: StateResponse): string {
    const lines = [`${selector.actorName}/${selector.actorId}`, `status: ${instance.status}`]
    if (instance.connections.length) {
        lines.push(`connections: ${instance.connections.length}`)
        for (const connection of instance.connections) lines.push(formatConnection(connection))
    }
    if (instance.waiting?.length) lines.push(`waiting: ${instance.waiting.map(item => item.operation).join(", ")}`)
    if (!state.snapshot) {
        lines.push(`No persisted snapshot for ${selector.actorName}/${selector.actorId}.`)
        return lines.join("\n")
    }
    lines.push(`stateVersion: ${state.snapshot.stateVersion}`, `requestId: ${state.snapshot.requestId}`)
    const attribution = state.snapshot.attribution
    if (
        attribution &&
        typeof attribution === "object" &&
        "operation" in attribution &&
        typeof attribution.operation === "string"
    )
        lines.push(`lastWrite: ${attribution.operation}`)
    lines.push(stringify(state.snapshot.state))
    return lines.join("\n")
}

function formatClass(row: Inventory["actors"][number]): string {
    const header = `${row.actorName}  live=${row.live}  dormant=${row.dormant}  unknown=${row.unknown}`
    return [header, ...row.instances.map(formatInstanceLine)].join("\n")
}

function formatInstanceLine(instance: ActorInstance): string {
    const details = [
        instance.connections.length ? `connections=${instance.connections.length}` : undefined,
        instance.waiting?.length ? `waiting=${instance.waiting.map(item => item.operation).join(",")}` : undefined
    ].filter(Boolean)
    return `  ${instance.actorId}  ${instance.status}${details.length ? `  ${details.join("  ")}` : ""}`
}

function formatConnection(connection: { id: string; metadata: unknown }): string {
    if (connection.metadata === undefined || connection.metadata === null) return `  ${connection.id}`
    return `  ${connection.id}  ${JSON.stringify(connection.metadata)}`
}

function findInstance(inventory: Inventory, selector: ActorSelector): ActorInstance | undefined {
    for (const row of inventory.actors) {
        if (row.actorName !== selector.actorName) continue
        const instance = row.instances.find(item => item.actorId === selector.actorId)
        if (instance) return instance
    }
    return undefined
}

function readInventory(value: unknown): Inventory {
    const result = inventorySchema.safeParse(value)
    if (!result.success) throw new Error("Invalid actor inventory response.")
    return result.data
}

function readState(value: unknown): StateResponse {
    const result = stateSchema.safeParse(value)
    if (!result.success) throw new Error("Invalid state response.")
    return result.data
}

function stringify(value: unknown): string {
    return JSON.stringify(value, null, 2)
}

export { formatInstance, formatInventory, inspectActors, parseActorSelector, registerInspectCommand }
