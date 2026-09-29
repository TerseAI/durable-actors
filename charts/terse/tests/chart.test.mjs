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

for (const [durability, placements] of [
    ["zonal", ["us-west4-a", "us-west4-a", "us-west4-a"]],
    ["regional", ["us-west4-a", "us-west4-b", "us-west4-c"]],
    ["multi_region", ["us-west4-a", "us-east4-a"]],
]) {
    test(`renders ${durability} replicas with persistent disks and HTTPS ingress`, () => {
        const result = render({ storage: { durability, replicas: { placements } } })
        assert.equal(result.status, 0, result.stderr)
        assert.match(result.stdout, /kind: Gateway/)
        assert.match(result.stdout, /volumeClaimTemplates:/)
        assert.match(result.stdout, /whenDeleted: Retain/)
        assert.match(result.stdout, /DURABLE_ACTORS_REPLICAS/)
        assert.match(result.stdout, /DURABLE_ACTORS_HOST_CPU_MILLIS, value: "250"/)
        assert.match(result.stdout, /DURABLE_ACTORS_HOST_MEMORY_MIB, value: "256"/)
        assert.equal((result.stdout.match(/kind: StatefulSet/g) ?? []).length, placements.length)
        assert.match(result.stdout, /automountServiceAccountToken: false/)
        assert.match(result.stdout, /port: 7200/)
    })
}
for (const [name, override] of [
    ["regional copies in one zone", { storage: { durability: "regional" } }],
    ["multi-region copies in one region", { storage: { durability: "multi_region" } }],
    ["empty replica set", { storage: { replicas: { placements: [] } } }],
    ["unknown policy", { storage: { durability: "best_effort" } }],
    ["mutable image", { image: { digest: "latest" } }],
    ["shared trust namespace", { sandboxNamespace: "terse-control" }],
]) test(`rejects ${name}`, () => assert.notEqual(render(override).status, 0))

test("replica count follows the placement list without a fixed maximum", () => {
    const result = render({ storage: { replicas: { placements: Array(9).fill("us-west4-a") } } })
    assert.equal(result.status, 0, result.stderr)
    assert.equal((result.stdout.match(/kind: StatefulSet/g) ?? []).length, 9)
})

test("regional installations deploy their selected replicas and permit configured remote networks", () => {
    const result = render({ storage: { durability: "multi_region", replicas: { placements: ["us-west4-a", "us-east4-a"], addresses: ["http://10.1.0.1:7200", "http://10.2.0.1:7200"], deployIndices: [0] } }, networkPolicy: { replicaCidrs: ["10.2.0.0/16"] } })
    assert.equal(result.status, 0, result.stderr)
    assert.equal((result.stdout.match(/kind: StatefulSet/g) ?? []).length, 1)
    assert.match(result.stdout, /cidr: "10.2.0.0\/16"/)
})

test("rejects out of range replica deployment indices", () => {
    assert.notEqual(render({ storage: { replicas: { deployIndices: [3] } } }).status, 0)
})

test("sandboxes can resolve DNS through kube-dns and GKE NodeLocal DNS pods", () => {
    const result = render()
    assert.equal(result.status, 0, result.stderr)
    const policy = result.stdout.split("---").find(document =>
        document.includes("kind: NetworkPolicy") && document.includes("namespace: terse-sandboxes"))
    assert.ok(policy)
    const dnsRule = policy.slice(policy.indexOf("kubernetes.io/metadata.name: kube-system"))
    const destinations = dnsRule.slice(0, dnsRule.indexOf("ports:"))
    assert.match(destinations, /kube-dns/)
    assert.match(destinations, /node-local-dns/)
    assert.match(dnsRule, /protocol: UDP, port: 53/)
    assert.match(dnsRule, /protocol: TCP, port: 53/)
})
