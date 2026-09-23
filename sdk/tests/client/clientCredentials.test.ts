import assert from "node:assert/strict"
import { test } from "node:test"

import { configuredSettings } from "../../src/client/clientSettings.js"

test("client settings require an API key and accept an explicit home assignment", () => {
    assert.throws(() => configuredSettings({ controlPlaneUrl: "https://actors.example", token: "delegated" }))
    assert.throws(() =>
        configuredSettings({ controlPlaneUrl: "https://actors.example", apiKey: "key", namespaceId: "tenant" })
    )
    assert.equal(
        configuredSettings({
            projectId: "default",
            controlPlaneUrl: "https://actors.example",
            apiKey: "key",
            homeRegion: "north-america-west"
        }).homeRegion,
        "north-america-west"
    )
})

test("client settings reject absent or empty project IDs", () => {
    for (const projectId of [undefined, "", ".", ".."])
        assert.throws(
            () => configuredSettings({ projectId, controlPlaneUrl: "https://actors.example", apiKey: "key" }),
            /projectId/
        )
})

test("loopback clients default the project and allow an omitted API key", () => {
    for (const host of ["127.0.0.1", "localhost", "[::1]"])
        for (const protocol of ["http", "https"]) {
            const settings = configuredSettings({ controlPlaneUrl: `${protocol}://${host}:7100` })
            assert.equal(settings.projectId, "local")
            assert.equal(settings.credential, undefined)
            assert.equal(
                configuredSettings({ controlPlaneUrl: `${protocol}://${host}:7100`, apiKey: "key" }).credential,
                "key"
            )
        }
})

test("remote clients still require both project IDs and API keys", () => {
    for (const host of ["actors.example", "localhost.example", "127.0.0.1.example", "192.168.1.1", "0.0.0.0"])
        for (const settings of [{}, { projectId: "local" }, { apiKey: "key" }])
            assert.throws(() => configuredSettings({ controlPlaneUrl: `http://${host}:7100`, ...settings }))
})
