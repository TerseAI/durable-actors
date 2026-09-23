import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorConfigurationError } from "../../src/errors.js"
import { parseHostSettings } from "../../src/host/actor-host.js"

test("a managed socket needs no local actor credentials", () => {
    const settings = parseHostSettings({
        DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-actors.sock",
        DURABLE_ACTORS_ENTRYPOINT: "dist/custom-actors.mjs"
    })

    assert.equal(settings.socketPath, "/tmp/durable-actors.sock")
    assert.equal(settings.actorEntrypoint, "dist/custom-actors.mjs")
    assert.equal(settings.startupTimeoutMs, 10_000)
})

test("actor-host startup timeout is configurable and bounded", () => {
    assert.equal(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-actors.sock",
            DURABLE_ACTORS_HOST_STARTUP_MS: "2500"
        }).startupTimeoutMs,
        2_500
    )
    assert.throws(
        () =>
            parseHostSettings({
                DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-actors.sock",
                DURABLE_ACTORS_HOST_STARTUP_MS: "0"
            }),
        ActorConfigurationError
    )
})

test("actor-host settings require a private session socket", () => {
    assert.throws(() => parseHostSettings({}), ActorConfigurationError)
})
