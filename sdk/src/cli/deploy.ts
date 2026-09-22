import { Command, Option } from "commander"
import { z } from "zod"

import { type ControlPlaneOptions, createControlPlaneClient } from "./control-plane.js"

interface DeployOptions extends ControlPlaneOptions {
    image: string
    workingDirectory: string
    secret: string[]
}

function registerDeployCommand(program: Command): void {
    program
        .command("deploy")
        .argument("[entrypoint]", "actor source in the image (default: src/durable-objects.ts)")
        .description("Deploy actors")
        .requiredOption("--image <reference>", "published actor image")
        .option("--working-directory <path>", "project directory in the image", "/customer")
        .option("--url <origin>", "control-plane origin (or DURABLE_OBJECT_CONTROL_PLANE_URL)")
        .addOption(new Option("--project-id <id>", "actor project ID").env("DURABLE_OBJECT_PROJECT_ID"))
        .option("--api-key <key>", "admin API key (or DURABLE_OBJECT_API_KEY)")
        .option(
            "--secret <name>",
            "Modal secret name (repeat for multiple secrets)",
            (value: string, previous: string[]) => [...previous, value],
            []
        )
        .action(deploy)
}

async function deploy(entrypoint = "src/durable-objects.ts", options: DeployOptions): Promise<void> {
    const client = createControlPlaneClient(options, fetch)
    const specification = deploymentSchema.parse({
        imageRef: options.image,
        workingDirectory: options.workingDirectory,
        actorEntrypoint: entrypoint,
        secretRefs: options.secret
    })
    const reply = z.object({ changed: z.boolean() }).parse(await client.registerDeployment(specification))
    console.log(reply.changed ? "Deployed actors." : "Deployment is up to date.")
}

const component = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const deploymentSchema = z.object({
    imageRef: z.string().startsWith("im-").min(4).max(255),
    workingDirectory: z.string().startsWith("/").max(1024),
    actorEntrypoint: z.string().min(1).max(1024),
    secretRefs: z.array(component.max(255)).max(16)
})

export { registerDeployCommand }
