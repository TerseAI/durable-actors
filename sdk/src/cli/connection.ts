import { Command, Option } from "commander"

import { configuredSettings } from "../client/clientSettings.js"

export interface ConnectionOptions {
    projectId: string
    url: string
    apiKey?: string
}

export function connectionOptions(command: Command): Command {
    return command
        .addOption(
            new Option("--project-id <id>", "actor project ID").env("DURABLE_OBJECT_PROJECT_ID").makeOptionMandatory()
        )
        .addOption(
            new Option("--url <origin>", "control-plane URL")
                .env("DURABLE_OBJECT_CONTROL_PLANE_URL")
                .default("http://127.0.0.1:7100")
        )
        .addOption(new Option("--api-key <key>", "admin API key").env("DURABLE_OBJECT_API_KEY"))
}

export function connection(options: ConnectionOptions) {
    if (!options.apiKey) throw new Error("Set --api-key or DURABLE_OBJECT_API_KEY to provide an admin API key.")
    return configuredSettings({ projectId: options.projectId, controlPlaneUrl: options.url, apiKey: options.apiKey })
}
