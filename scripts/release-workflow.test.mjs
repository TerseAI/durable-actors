import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const root = new URL("../", import.meta.url)
const read = path => readFileSync(new URL(path, root), "utf8")

test("release images use the established Terse Artifact Registry", () => {
    const workflow = read(".github/workflows/release.yml")

    assert.match(workflow, /REGISTRY: us-central1-docker\.pkg\.dev/)
    assert.match(workflow, /IMAGE: us-central1-docker\.pkg\.dev\/fluid-analogy-473415-c2\/public\/little-actors/)
    assert.match(workflow, /google-github-actions\/auth@/)
    assert.match(workflow, /actions\/attest@/)
    assert.doesNotMatch(workflow, /push-to-registry: true/)
    assert.doesNotMatch(workflow, /ghcr\.io/)
})

test("npm publishes the downloaded tarball as a filesystem path", () => {
    const workflow = read(".github/workflows/release.yml")

    assert.match(workflow, /npm publish \.\/dist-tarballs\/\*\.tgz --access public/)
})

test("runtime images include the one-shot Go provider", () => {
    const dockerfile = read("Dockerfile")
    assert.match(dockerfile, /FROM golang:1\.27\.1-bookworm AS modal-builder/)
    assert.match(dockerfile, /COPY providers\/modal-go\/ /)
    assert.match(dockerfile, /CGO_ENABLED=0 go build -mod=readonly -trimpath/)
    assert.match(dockerfile, /COPY --from=modal-builder .* \/usr\/local\/bin\/little-actors-modal-go/)
    assert.match(dockerfile, /DURABLE_OBJECT_SANDBOX_COMMAND=little-actors-modal-go/)
    assert.match(read(".dockerignore"), /!providers\/modal-go\/\*\*/)
})

test("CI and release validate the Go provider before publishing", () => {
    for (const path of [".github/workflows/ci.yml", ".github/workflows/release.yml"]) {
        const workflow = read(path)
        assert.match(workflow, /working-directory: providers\/modal-go/)
        assert.match(workflow, /go test -race -mod=readonly \.\/\.\.\./)
        assert.match(workflow, /go-version: "1\.27\.1"/)
    }
    assert.match(read(".github/workflows/release.yml"), /needs: \[preflight, rust, npm-ci, go-ci\]/)
})

test("CI and release exercise direct host sockets with the built SDK", () => {
    for (const path of [".github/workflows/ci.yml", ".github/workflows/release.yml"]) {
        const rustJob = read(path)
            .split("    rust:\n")[1]
            .split(/\n    [a-z-]+:\n/)[0]
        assert.match(rustJob, /pnpm --dir sdk build[\s\S]*cargo test --locked -- --ignored/)
    }
})
