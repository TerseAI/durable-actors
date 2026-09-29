import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdtempSync, writeFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"

const chart = new URL("../", import.meta.url).pathname
const helm = process.env.HELM ?? "helm"
function render(overrides = {}) {
    const directory = mkdtempSync(join(tmpdir(), "terse-chart-"))
    try {
        const values = join(directory, "values.json")
        writeFileSync(values, JSON.stringify(overrides))
        return spawnSync(helm, ["template", "test", chart, "--namespace", "terse-control", "-f", `${chart}tests/values.yaml`, "-f", values], { encoding: "utf8" })
    } finally { rmSync(directory, { recursive: true }) }
}

for (const [durability, zones] of [
    ["zonal", ["us-west4-a"]],
    ["regional", ["us-west4-a", "us-west4-b"]],
    ["multi_region", ["us-west4-a", "us-east4-a"]],
]) {
    test(`renders ${durability} storage with a private sandbox namespace and HTTPS ingress`, () => {
        const result = render({ storage: { durability, rapidBuckets: zones.map((zone, i) => ({ name: `test-copy-${i}`, zone })) } })
        assert.equal(result.status, 0, result.stderr)
        assert.match(result.stdout, /kind: Gateway/)
        assert.match(result.stdout, /kind: NetworkPolicy/)
        assert.match(result.stdout, /automountServiceAccountToken: false/)
        assert.match(result.stdout, /DURABLE_ACTORS_RAPID_BUCKETS/)
    })
}
for (const [name, override] of [
    ["regional copies in one zone", { storage: { durability: "regional" } }],
    ["multi-region copies in one region", { storage: { durability: "multi_region" } }],
    ["duplicate buckets", { storage: { rapidBuckets: [{name:"same",zone:"us-west4-a"},{name:"same",zone:"us-west4-a"}] } }],
    ["unknown policy", { storage: { durability: "best_effort" } }],
    ["mutable image", { image: { digest: "latest" } }],
    ["shared trust namespace", { sandboxNamespace: "terse-control" }],
    ["compute without a local copy", { zones: { "north-america-west": "us-west4-b" } }],
]) test(`rejects ${name}`, () => assert.notEqual(render(override).status, 0))
