import { Command, Option } from "commander"
import { randomUUID } from "node:crypto"
import { z } from "zod"

import { type ControlPlaneOptions, createControlPlaneClient } from "./control-plane.js"

interface DeployOptions extends ControlPlaneOptions {
    image: string
    revision?: string
    workingDirectory: string
    secret: string[]
}

function registerDeployCommand(program: Command): void {
    program
        .command("deploy [entrypoint]")
        .description("Deploy actor source from a published customer image")
        .requiredOption("--image <reference>", "published customer build image reference")
        .option("--working-directory <path>", "project directory inside the build image", "/customer")
        .option("--revision <revision>", "code revision (defaults to a new generated ID)")
        .option("--url <origin>", "control-plane origin (or DURABLE_ACTORS_CONTROL_PLANE_URL)")
        .addOption(new Option("--project-id <id>", "actor project ID").env("DURABLE_ACTORS_PROJECT_ID"))
        .option("--api-key <key>", "shared secret (or DURABLE_ACTORS_SECRET)")
        .option(
            "--secret <name>",
            "Modal secret reference (uses an on-demand sandbox)",
            (value: string, previous: string[]) => [...previous, value],
            []
        )
        .action(deploy)
}

async function deploy(entrypoint = "src/durable-objects.ts", options: DeployOptions): Promise<void> {
    const client = createControlPlaneClient(options, fetch)
    const specification = deploymentSchema.parse({
        codeRevision: options.revision ?? randomUUID(),
        imageRef: options.image,
        workingDirectory: options.workingDirectory,
        actorEntrypoint: entrypoint,
        secretRefs: options.secret
    })
    const reply = z.object({ changed: z.boolean() }).parse(await client.registerDeployment(specification))
    console.log(`${reply.changed ? "Registered" : "Already registered"} revision ${specification.codeRevision}.`)
}

const component = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const deploymentSchema = z.object({
    codeRevision: component.max(128),
    imageRef: z.string().startsWith("im-").min(4).max(255),
    workingDirectory: z.string().startsWith("/").max(1024),
    actorEntrypoint: z.string().min(1).max(1024),
    secretRefs: z.array(component.max(255)).max(16)
})

export { registerDeployCommand }
