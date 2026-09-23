import { configuredSettings } from "../client/clientSettings.js"
import { actorEnvironment } from "../environment.js"

export function connection(environment: NodeJS.ProcessEnv) {
    const env = actorEnvironment(environment)
    return configuredSettings({
        projectId: env.DURABLE_ACTORS_PROJECT_ID,
        controlPlaneUrl: env.DURABLE_ACTORS_CONTROL_PLANE_URL || "http://127.0.0.1:7100",
        apiKey: env.DURABLE_ACTORS_SECRET
    })
}

export const connectionHelp = `
Connection settings (.env or environment):
  DURABLE_ACTORS_PROJECT_ID (default: local for loopback connections)
  DURABLE_ACTORS_CONTROL_PLANE_URL (default: http://127.0.0.1:7100)
  DURABLE_ACTORS_SECRET (optional locally; enables authentication when set)`
