import { configuredSettings } from "../client/clientSettings.js"

export function connection(env: NodeJS.ProcessEnv) {
    if (!env.DURABLE_OBJECT_PROJECT_ID) throw new Error("Set DURABLE_OBJECT_PROJECT_ID to your actor project ID.")
    if (!env.DURABLE_OBJECT_API_KEY) throw new Error("Set DURABLE_OBJECT_API_KEY to provide an admin API key.")
    return configuredSettings({
        projectId: env.DURABLE_OBJECT_PROJECT_ID,
        controlPlaneUrl: env.DURABLE_OBJECT_CONTROL_PLANE_URL || "http://127.0.0.1:7100",
        apiKey: env.DURABLE_OBJECT_API_KEY
    })
}

export const connectionHelp = `
Connection settings (.env or environment):
  DURABLE_OBJECT_PROJECT_ID
  DURABLE_OBJECT_CONTROL_PLANE_URL (default: http://127.0.0.1:7100)
  DURABLE_OBJECT_API_KEY`
