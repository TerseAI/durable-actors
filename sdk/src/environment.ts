export function actorEnvironment(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
    const resolved = { ...environment }
    for (const [name, value] of Object.entries(environment)) {
        if (name.startsWith("DURABLE_OBJECT_") && value !== undefined)
            resolved[name.replace("DURABLE_OBJECT_", "DURABLE_ACTORS_")] ??= value
    }
    const secret = resolved.DURABLE_ACTORS_SECRET ?? resolved.DURABLE_ACTORS_API_KEY
    if (secret !== undefined) {
        resolved.DURABLE_ACTORS_SECRET = secret
        resolved.DURABLE_ACTORS_API_KEY = secret
    }
    for (const [name, value] of Object.entries(resolved)) {
        if (name.startsWith("DURABLE_ACTORS_") && value !== undefined)
            resolved[name.replace("DURABLE_ACTORS_", "DURABLE_OBJECT_")] = value
    }
    return resolved
}
