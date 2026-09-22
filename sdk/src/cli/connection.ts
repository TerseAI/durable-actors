import { configuredSettings } from "../client/clientSettings.js"
import { actorEnvironment } from "../environment.js"

export function connection(environment: NodeJS.ProcessEnv) {
    const env = actorEnvironment(environment)
    if (!env.DURABLE_ACTORS_PROJECT_ID) throw new Error("Set DURABLE_ACTORS_PROJECT_ID to your actor project ID.")
    if (!env.DURABLE_ACTORS_SECRET) throw new Error("Set DURABLE_ACTORS_SECRET to provide the shared secret.")
    return configuredSettings({
        projectId: env.DURABLE_ACTORS_PROJECT_ID,
        controlPlaneUrl: env.DURABLE_ACTORS_CONTROL_PLANE_URL || "http://127.0.0.1:7100",
        apiKey: env.DURABLE_ACTORS_SECRET
    })
}

export const connectionHelp = `
Connection settings (.env or environment):
  DURABLE_ACTORS_PROJECT_ID
  DURABLE_ACTORS_CONTROL_PLANE_URL (default: http://127.0.0.1:7100)
  DURABLE_ACTORS_SECRET`
