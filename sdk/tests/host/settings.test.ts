import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorConfigurationError } from "../../src/errors.js"
import { parseHostSettings } from "../../src/host/actor-host.js"

test("a managed socket needs no local actor credentials", () => {
    const settings = parseHostSettings({
        DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
        DURABLE_ACTORS_ENTRYPOINT: "src/custom-actors.ts"
    })

    assert.equal(settings.socketPath, "/tmp/durable-object.sock")
    assert.equal(settings.actorEntrypoint, "src/custom-actors.ts")
    assert.equal(settings.startupTimeoutMs, 10_000)
    assert.equal(settings.actorIdleTimeoutMs, 60_000)
})

test("resident actor idle timeout uses seconds and is bounded", () => {
    assert.equal(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
            DURABLE_ACTORS_ACTOR_IDLE_TIMEOUT_SECONDS: "10"
        }).actorIdleTimeoutMs,
        10_000
    )
    for (const value of ["0", "-1", "1.5", "86401", "not-a-number"]) {
        assert.throws(
            () =>
                parseHostSettings({
                    DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
                    DURABLE_ACTORS_ACTOR_IDLE_TIMEOUT_SECONDS: value
                }),
            ActorConfigurationError
        )
    }
})

test("actor-host startup timeout is configurable and bounded", () => {
    assert.equal(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
            DURABLE_ACTORS_HOST_STARTUP_MS: "2500"
        }).startupTimeoutMs,
        2_500
    )
    assert.throws(
        () =>
            parseHostSettings({
                DURABLE_ACTORS_EXECUTOR_SOCKET: "/tmp/durable-object.sock",
                DURABLE_ACTORS_HOST_STARTUP_MS: "0"
            }),
        ActorConfigurationError
    )
})

test("actor-host settings require a private session socket", () => {
    assert.throws(() => parseHostSettings({}), ActorConfigurationError)
})
