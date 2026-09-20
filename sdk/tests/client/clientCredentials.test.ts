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
            controlPlaneUrl: "https://actors.example",
            apiKey: "key",
            homeRegion: "north-america-west"
        }).homeRegion,
        "north-america-west"
    )
})
