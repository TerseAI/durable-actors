import { expect, test } from "bun:test"
import path from "node:path"

import { stagingExamplePath } from "../../scripts/staging.mjs"

test("staging resolves grouped actor projects", () => {
    expect(stagingExamplePath("/repo", "persistence/persistence-tests")).toBe(path.join("/repo", "examples", "persistence", "persistence-tests"))
    expect(stagingExamplePath("/repo", "chat")).toBe(path.join("/repo", "examples", "chat"))
})

test("staging rejects paths outside the example tree", () => {
    for (const name of [undefined, "", "../sdk", "/chat", "persistence/../chat", "persistence//cli"]) expect(() => stagingExamplePath("/repo", name)).toThrow("Usage:")
})
