import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"

const chart = new URL("../", import.meta.url).pathname
const helm = process.env.HELM ?? "helm"

test("Substrate shares worker capacity and projects rotating API credentials", () => {
    const result = render({ substrate: { worker: { replicas: 2, cpu: "4", memory: "8Gi" } } })
    assert.equal(result.status, 0, result.stderr)
    const pool = result.stdout.split("---").find(doc => doc.includes("kind: WorkerPool"))
    assert.ok(pool)
    assert.match(pool, /replicas: 2/)
    assert.match(pool, /cpu: "4"/)
    assert.match(pool, /memory: "8Gi"/)
    assert.match(result.stdout, /audience: api.ate-system.svc/)
    assert.match(result.stdout, /clusterTrustBundle:/)
    assert.match(result.stdout, /DURABLE_ACTORS_SUBSTRATE_ENDPOINT/)
})

function render(overrides = {}) {
    const directory = mkdtempSync(join(tmpdir(), "terse-chart-"))
    try {
        const values = join(directory, "values.json")
        writeFileSync(values, JSON.stringify(overrides))
        return spawnSync(helm, ["template", "test", chart, "--namespace", "terse-control", "-f", `${chart}tests/values.yaml`, "-f", values], { encoding: "utf8" })
    } finally {
        rmSync(directory, { recursive: true })
    }
}

test("renders two Rapid zones and the Standard archive", () => {
    const result = render()
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.stdout, /DURABLE_ACTORS_ARCHIVE_BUCKET, value: "test-archive"/)
    const buckets = result.stdout.split("\n").find(line => line.includes("DURABLE_ACTORS_RAPID_BUCKETS"))
    for (const value of ["test-rapid-a", "test-rapid-b", "us-west4-a", "us-west4-b"]) assert.ok(buckets.includes(value))
    const deployment = result.stdout.split("---").find(document => document.includes("kind: Deployment\n"))
    assert.match(deployment, /replicas: 2\b/)
    assert.match(result.stdout, /kind: Gateway/)
    assert.match(result.stdout, /whenUnsatisfiable: DoNotSchedule/)
})

for (const [name, overrides, bytes, interval] of [
    ["default", {}, 16777216, 10000],
    ["configured", { storage: { rapid: { archiveBatchBytes: 2097152, archiveBatchIntervalMs: 250 } } }, 2097152, 250]
]) test(`renders ${name} archive batch triggers`, () => {
    const result = render(overrides)
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.stdout, new RegExp(`DURABLE_ACTORS_ARCHIVE_BATCH_BYTES, value: "${bytes}"`))
    assert.match(result.stdout, new RegExp(`DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS, value: "${interval}"`))
})

for (const field of ["archiveBatchBytes", "archiveBatchIntervalMs"])
    test(`rejects nonpositive ${field}`, () => assert.notEqual(render({ storage: { rapid: { [field]: 0 } } }).status, 0))

for (const [name, override] of [
    ["single control plane", { replicaCount: 1 }],
    ["duplicate Rapid zones", { storage: { rapid: { buckets: [{ bucket: "rapid-one", zone: "us-west4-a" }, { bucket: "rapid-two", zone: "us-west4-a" }] } } }],
    ["single Rapid zone", { storage: { rapid: { buckets: [{ bucket: "rapid-one", zone: "us-west4-a" }] } } }],
    ["empty Rapid set", { storage: { rapid: { buckets: [] } } }],
    ["mutable image", { image: { digest: "latest" } }],
    ["invalid worker memory", { substrate: { worker: { memory: "" } } }]
]) test(`rejects ${name}`, () => assert.notEqual(render(override).status, 0))

test("uses a Google-managed certificate on the HTTPS gateway", () => {
    const result = render({ gateway: { tlsSecret: "", preSharedCert: "actors-production" } })
    assert.equal(result.status, 0, result.stderr)
    const gateway = result.stdout.split("---").find(document => document.includes("kind: Gateway"))
    assert.match(gateway, /networking\.gke\.io\/pre-shared-certs: "actors-production"/)
    assert.doesNotMatch(gateway, /certificateRefs/)
})

for (const [name, gateway] of [
    ["missing TLS certificate", { tlsSecret: "", preSharedCert: "" }],
    ["ambiguous TLS certificates", { tlsSecret: "tls", preSharedCert: "managed" }]
])
    test(`rejects ${name}`, () => assert.notEqual(render({ gateway }).status, 0))

test("Cloud SQL connects through a private local proxy that starts before the control plane", () => {
    const result = render({ cloudSql: { instanceConnectionName: "project:us-west4:actors-db" } })
    assert.equal(result.status, 0, result.stderr)
    const deployment = result.stdout.split("---").find(document => document.includes("kind: Deployment"))
    assert.match(deployment, /initContainers:[\s\S]*name: cloud-sql-proxy[\s\S]*restartPolicy: Always/)
    assert.match(deployment, /--address=127\.0\.0\.1/)
    assert.match(deployment, /--private-ip/)
    assert.match(deployment, /project:us-west4:actors-db/)
    assert.match(deployment, /path: \/startup/)
    assert.match(deployment, /key: postgres-url/)
})
