import assert from "node:assert/strict"
import { test } from "node:test"

import { configuredSettings } from "../../src/client/clientSettings.js"

test("client settings validate supported options and accept an explicit home assignment", () => {
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

test("localhost clients default the project and allow an omitted API key", () => {
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

test("remote clients require a project ID and allow an omitted secret", () => {
    for (const host of ["actors.example", "localhost.example", "127.0.0.1.example", "192.168.1.1", "0.0.0.0"])
        for (const apiKey of [undefined, "key"]) {
            const options = { controlPlaneUrl: `http://${host}:7100`, apiKey }
            assert.throws(() => configuredSettings(options), /projectId/)
            const settings = configuredSettings({ ...options, projectId: "my-project" })
            assert.equal(settings.projectId, "my-project")
            assert.equal(settings.credential, apiKey)
        }
})
