import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const root = new URL("../../", import.meta.url)
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
        assert.match(workflow, /go test -race -mod=readonly -overlay tests\/overlay\.json \.\/\.\.\./)
        assert.match(workflow, /go-version: "1\.27\.1"/)
    }
    for (const job of ["native-publish", "image-push", "image", "npm", "crate"]) {
        for (const check of ["rust", "npm-ci", "go-ci"]) assert.ok(dependsOn(job, check), `${job} must wait for ${check}`)
    }
})

test("CI and release exercise direct host sockets with the built SDK", () => {
    for (const path of [".github/workflows/ci.yml", ".github/workflows/release.yml"]) {
        const rustJob = read(path)
            .split("    rust:\n")[1]
            .split(/\n    [a-z-]+:\n/)[0]
        assert.match(rustJob, /pnpm --dir sdk build[\s\S]*cargo test --locked -- --ignored/)
    }
})

test("runtime image generates protobuf sources before compiling the SDK", () => {
    const dockerfile = read("Dockerfile")
    assert.match(dockerfile, /pnpm --dir sdk generate:proto[\s\S]*pnpm --dir sdk exec tsc/)
})

test("the SDK is packed once after validation and reused by native tests and npm", () => {
    const validation = releaseJob("npm-ci")
    assert.match(validation, /oven-sh\/setup-bun@v2/)
    assert.match(validation, /pnpm --dir sdk package:check/)
    assert.match(validation, /pnpm --dir sdk --config.ignore-scripts=true pack/)
    assert.match(validation, /actions\/upload-artifact@[\s\S]*name: sdk-package/)
    assert.equal(read(".github/workflows/release.yml").match(/pack --pack-destination/g)?.length, 1)
    for (const job of ["native-publish", "npm"]) {
        assert.match(releaseJob(job), /actions\/download-artifact@[\s\S]*name: sdk-package/)
        assert.doesNotMatch(releaseJob(job), /pnpm (build|--dir sdk (build|pack))/)
    }
    assert.match(releaseJob("native-publish"), /DURABLE_OBJECT_TEST_PACKAGE:[\s\S]*examples\/chat build/)
})

test("native and image builds start independently of validation and stage artifacts", () => {
    for (const job of ["native", "image-build"]) {
        assert.deepEqual(dependencies(job), ["preflight"])
        assert.match(releaseJob(job), /actions\/upload-artifact@/)
        assert.doesNotMatch(releaseJob(job), /push: true|gh release upload|docker push/)
    }
    assert.match(releaseJob("native"), /node scripts\/build-runtime.mjs/)
    assert.match(releaseJob("image-build"), /outputs: type=docker,dest=/)
    assert.ok(dependsOn("native-publish", "native"))
    assert.ok(dependsOn("image-push", "image-build"))
})

test("Cargo caches are restored after toolchain selection", () => {
    for (const job of ["rust", "native"]) assert.match(releaseJob(job), /rustup default 1\.89\.0[\s\S]*Swatinem\/rust-cache@/)
    assert.match(read(".github/workflows/ci.yml"), /uses: \.\/\.github\/workflows\/native-build.yml/)
    assert.match(read(".github/workflows/native-build.yml"), /shared-key: native-\$\{\{ matrix.runner \}\}/)
    const dockerfile = read("Dockerfile")
    assert.match(dockerfile, /--mount=type=cache,target=\/usr\/local\/cargo/)
    assert.match(dockerfile, /--mount=type=cache,target=\/build\/target/)
    assert.match(dockerfile, /cp target\/release\/little-actors \/out\/little-actors/)
    assert.match(dockerfile, /COPY --from=builder \/out\/little-actors/)
})

test("crate publication reuses successful verification and does not wait for npm publication", () => {
    assert.match(releaseJob("rust"), /cargo publish --locked --dry-run/)
    assert.ok(dependsOn("crate", "rust"))
    assert.match(releaseJob("crate"), /cargo publish --locked --no-verify/)
    assert.ok(!dependsOn("crate", "npm"))
})

function releaseJob(name) {
    const body = read(".github/workflows/release.yml").split(`    ${name}:\n`)[1]
    assert.ok(body, `Missing release job: ${name}`)
    const job = body.split(/\n    [a-z-]+:\n/)[0]
    const reusable = job.match(/^        uses: \.\/(.+)$/m)?.[1]
    return reusable ? `${job}\n${read(reusable)}` : job
}

function dependencies(name) {
    return (
        releaseJob(name)
            .match(/^        needs: (.+)$/m)?.[1]
            .replace(/[\[\]]/g, "")
            .split(/,\s*/) ?? []
    )
}

function dependsOn(job, dependency) {
    return dependencies(job).some(name => name === dependency || dependsOn(name, dependency))
}
