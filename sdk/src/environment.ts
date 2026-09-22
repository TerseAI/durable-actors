export function actorEnvironment(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
    const resolved = { ...environment }
    const secret = resolved.DURABLE_ACTORS_SECRET ?? resolved.DURABLE_ACTORS_API_KEY
    if (secret !== undefined) {
        resolved.DURABLE_ACTORS_SECRET = secret
        resolved.DURABLE_ACTORS_API_KEY = secret
    }
    return resolved
}
