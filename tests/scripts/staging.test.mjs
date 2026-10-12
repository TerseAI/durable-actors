import assert from "node:assert/strict"
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"

import { Staging, stagingEnvironment } from "../../scripts/staging.mjs"

async function fixture(t) {
    const root = await mkdtemp(path.join(tmpdir(), "staging-example-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const template = (await readFile(new URL("../../.env.staging.example", import.meta.url), "utf8")).replace("STAGING_SECRET_KEYS=", "STAGING_SECRET_KEYS=OPENAI_API_KEY") + "\nOPENAI_API_KEY=\n"
    const document = template.replace(/^([A-Z_]+)=$/gm, (_, key) => `${key}=${key === "DURABLE_ACTORS_POSTGRES_URL" ? "postgresql://test:test@127.0.0.1:5432/staging" : `staging-${key}`}`)
    await writeFile(path.join(root, ".env.staging"), document)
    await mkdir(path.join(root, "examples/persistence-tests/src"), { recursive: true })
    await writeFile(path.join(root, "examples/persistence-tests/src/actors.ts"), "")
    return { root, document }
}

test("staging settings ignore ambient credentials and normal dotenv files", async t => {
    const { root, document } = await fixture(t)
    await writeFile(path.join(root, ".env"), "OPENAI_API_KEY=normal-key\n")
    const env = stagingEnvironment(document, {
        PATH: "/tools",
        HOME: "/home/test",
        OPENAI_API_KEY: "production-key",
        DURABLE_ACTORS_SECRET: "production-secret",
        GOOGLE_APPLICATION_CREDENTIALS: "/production.json",
        CLOUDSDK_AUTH_ACCESS_TOKEN: "production-token",
        NODE_OPTIONS: "--require=/inject.js"
    })
    assert.equal(env.OPENAI_API_KEY, "staging-OPENAI_API_KEY")
    assert.equal(env.DURABLE_ACTORS_SECRET, "staging-DURABLE_ACTORS_SECRET")
    assert.equal(env.CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE, env.GOOGLE_APPLICATION_CREDENTIALS)
    assert.equal(env.NGROK_AUTHTOKEN, "staging-NGROK_AUTH_TOKEN")
    assert.equal(env.CLOUDSDK_AUTH_ACCESS_TOKEN, undefined)
    assert.equal(env.NODE_OPTIONS, undefined)
    assert.equal(env.PATH, "/tools")
})

test("missing staging keys fail even if the shell supplies them", async t => {
    const { document } = await fixture(t)
    assert.throws(() => stagingEnvironment(document.replace(/^OPENAI_API_KEY=.*\n/m, ""), { OPENAI_API_KEY: "ambient" }), /OPENAI_API_KEY.*\.env.staging/)
})

test("staging can use separate explicit credentials for setup tools and the control plane", async t => {
    const { document } = await fixture(t)
    const env = stagingEnvironment(`${document}\nCLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=/staging/user.json\n`, {
        CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE: "/ambient/credentials.json"
    })
    assert.equal(env.CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE, "/staging/user.json")
    assert.equal(env.GOOGLE_APPLICATION_CREDENTIALS, "/absolute/path/to/staging-credentials.json")
})

test("staging rejects production cluster and bucket destinations", async t => {
    const { document } = await fixture(t)
    assert.throws(() => stagingEnvironment(document.replace("STAGING_GKE_CLUSTER=your-staging-cluster", "STAGING_GKE_CLUSTER=production-cluster")), /staging cluster/)
    assert.throws(() => stagingEnvironment(document.replace("DURABLE_ACTORS_BUCKET=your-staging-authority", "DURABLE_ACTORS_BUCKET=production")), /staging bucket/)
    assert.throws(() => stagingEnvironment(document.replace("STAGING_GKE_CLUSTER=your-staging-cluster", "STAGING_GKE_CLUSTER=production-staging")), /staging cluster/)
    assert.throws(() => stagingEnvironment(document.replace("DURABLE_ACTORS_GKE_NAMESPACE=your-staging-actor-tests", "DURABLE_ACTORS_GKE_NAMESPACE=production")), /staging test namespace/)
    assert.throws(() => stagingEnvironment(document.replace(".pkg.dev/your-staging-project/", ".pkg.dev/another-project/")), /STAGING_GCP_PROJECT/)
})

test("server startup isolates dotenv files and generates its own tunnel config", async t => {
    const { root, document } = await fixture(t)
    await writeFile(path.join(root, ".env"), "OPENAI_API_KEY=wrong\n")
    const env = stagingEnvironment(`${document}\nNGROK_CONFIG=.little-actors/ngrok.yml\n`)
    const launcher = new Staging(root, env, path.join(root, "examples/persistence-tests"))
    const options = await launcher.serverOptions()
    await assert.rejects(readFile(path.join(options.cwd, ".env")), { code: "ENOENT" })
    assert.equal(path.dirname(options.cwd), launcher.work)
    assert.equal(options.env.NGROK_CONFIG, path.join(options.cwd, "ngrok.yml"))
    assert.equal(await readFile(options.env.NGROK_CONFIG, "utf8"), 'version: "3"\nagent:\n  web_addr: false\n')
    assert.equal(options.env.OPENAI_API_KEY, env.OPENAI_API_KEY)
})

test("failed preflight cannot create cloud resources or start the server", async t => {
    const { root, document } = await fixture(t)
    const calls = []
    const launcher = new Staging(root, stagingEnvironment(document), path.join(root, "examples/persistence-tests"), async (command, args) => {
        calls.push({ command, args })
        throw new Error("preflight failed")
    })
    await assert.rejects(launcher.start(), /preflight failed/)
    assert.equal(calls.length, 1)
    assert.ok(!calls[0].args.includes("create"))
})

test("a kubeconfig pointed at another cluster fails before any cloud changes", async t => {
    const { root, document } = await fixture(t)
    const calls = []
    const launcher = new Staging(root, stagingEnvironment(document), path.join(root, "examples/persistence-tests"), async (command, args) => {
        calls.push({ command, args })
        if (args.includes("describe")) return JSON.stringify({ endpoint: "10.0.0.1" })
        if (args.includes("view")) return JSON.stringify({ clusters: [{ cluster: { server: "https://10.0.0.2" } }] })
        return "/google-sdk"
    })
    await assert.rejects(launcher.preflight(), /staging cluster endpoint/)
    assert.ok(calls.every(call => !call.args.includes("create") && !call.args.includes("apply")))
})

test("repeated setup reuses its cloud resources and sends only the configured key to the actor Secret", async t => {
    const { root, document } = await fixture(t)
    const env = stagingEnvironment(document)
    const calls = []
    const launcher = new Staging(root, env, path.join(root, "examples/persistence-tests"), async (command, args, options) => {
        calls.push({ command, args, options })
        if (args.includes("repositories")) return JSON.stringify([{ name: "projects/test/locations/us-central1/repositories/your-staging-images" }])
        if (args.includes("node-pools")) return JSON.stringify([{ name: env.STAGING_NODE_POOL, config: { sandboxConfig: { type: "GVISOR" } } }])
        return ""
    })
    await launcher.prepareInfrastructure()
    assert.ok(calls.every(call => !call.args.includes("create")))
    assert.ok(calls.find(call => call.args.includes("repositories")).args.includes("--location=us-central1"))
    const applied = calls.find(call => call.args.includes("apply"))
    const secret = JSON.parse(applied.options.input).items.find(item => item.kind === "Secret")
    assert.deepEqual(secret.stringData, { OPENAI_API_KEY: env.OPENAI_API_KEY })
    assert.equal(secret.metadata.namespace, "your-staging-actor-tests")
    assert.ok(applied.args.includes(`--context=${launcher.context}`))
    for (const call of calls) assert.equal(call.options.env.CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE, env.GOOGLE_APPLICATION_CREDENTIALS)
})

test("image builds retain Docker tooling but isolate credentials and refresh login after the build", async t => {
    const { root, document } = await fixture(t)
    const env = stagingEnvironment(document)
    const digest = `sha256:${"a".repeat(64)}`
    const calls = []
    const launcher = new Staging(root, env, path.join(root, "examples/persistence-tests"), async (command, args, options) => {
        calls.push({ command, args, options })
        if (args.includes("info")) return JSON.stringify([{ Name: "buildx", Path: "/docker/plugins/docker-buildx" }])
        if (args.includes("context")) return JSON.stringify("unix:///docker/run/docker.sock")
        if (args.includes("print-access-token")) return "staging-access-token\n"
        if (args.includes("buildx")) {
            const config = JSON.parse(await readFile(path.join(options.env.DOCKER_CONFIG, "config.json"), "utf8"))
            assert.deepEqual(config, { cliPluginsExtraDirs: ["/docker/plugins"] })
            assert.equal(options.env.DOCKER_HOST, "unix:///docker/run/docker.sock")
        }
        if (args.includes("inspect")) return JSON.stringify([`${env.STAGING_IMAGE_REPOSITORY}@${digest}`])
        return ""
    })
    assert.equal(await launcher.buildRuntimeImage(), `${env.STAGING_IMAGE_REPOSITORY}@${digest}`)
    assert.ok(calls.findIndex(call => call.args.includes("buildx")) < calls.findIndex(call => call.args.includes("print-access-token")))
    assert.equal(calls.find(call => call.args.includes("login")).options.input, "staging-access-token")
    assert.equal(calls.find(call => call.args.includes("login")).args[1], "us-central1-docker.pkg.dev")
    const saved = JSON.parse(await readFile(path.join(launcher.work, "docker/config.json"), "utf8"))
    assert.equal(saved.auths, undefined)
})

test("builds the selected example and keeps its output separate", async t => {
    const { root, document } = await fixture(t)
    const env = stagingEnvironment(document)
    const example = path.join(root, "examples/chat")
    const calls = []
    const contract = { actors: [] }
    const digest = `${env.STAGING_IMAGE_REPOSITORY}@sha256:${"a".repeat(64)}`
    const launcher = new Staging(root, env, example, async (command, args) => {
        calls.push({ command, args })
        if (args[0] === path.join(root, "sdk/dist/compiler/deployment-build.js")) return JSON.stringify(contract)
        if (args.includes("info")) return JSON.stringify([{ Name: "buildx", Path: "/docker/plugins/docker-buildx" }])
        if (args.includes("context")) return JSON.stringify("unix:///docker/run/docker.sock")
        if (args.includes("inspect")) return JSON.stringify([digest])
        if (args.includes("describe")) return JSON.stringify({ generation: "123" })
        return ""
    })
    await mkdir(launcher.work, { recursive: true })
    await writeFile(path.join(launcher.work, "actors.mjs"), "compiled actor")
    const deployment = await launcher.build()
    const compiler = calls.find(call => call.args[0] === path.join(root, "sdk/dist/compiler/deployment-build.js"))
    assert.deepEqual(compiler.args.slice(1), [example, "src/actors.ts", launcher.work])
    assert.equal(launcher.work, path.join(root, ".durable-actors/staging/chat"))
    assert.deepEqual(deployment.contract, contract)
    assert.deepEqual(deployment.secretRefs, ["staging-chat"])
})

test("examples receive only their configured secret keys", async t => {
    const { root, document } = await fixture(t)
    const env = stagingEnvironment(document.replaceAll("STAGING_SECRET_KEYS=OPENAI_API_KEY", "STAGING_SECRET_KEYS=DEMO_TOKEN") + "\nDEMO_TOKEN=demo-secret\n")
    const calls = []
    const launcher = new Staging(root, env, path.join(root, "examples/chat"), async (command, args, options) => {
        calls.push({ command, args, options })
        if (args.includes("repositories")) return JSON.stringify([{ name: "projects/test/locations/us-central1/repositories/your-staging-images" }])
        if (args.includes("node-pools")) return JSON.stringify([{ name: env.STAGING_NODE_POOL, config: { sandboxConfig: { type: "GVISOR" } } }])
        return ""
    })
    await launcher.prepareInfrastructure()
    const secret = JSON.parse(calls.find(call => call.args.includes("apply")).options.input).items.find(item => item.kind === "Secret")
    assert.equal(secret.metadata.name, "staging-chat")
    assert.deepEqual(secret.stringData, { DEMO_TOKEN: "demo-secret" })
})

test("examples without secrets do not require an OpenAI key", async t => {
    const { document } = await fixture(t)
    const env = stagingEnvironment(document.replace(/^STAGING_SECRET_KEYS=.*$/gm, "STAGING_SECRET_KEYS=").replace(/^OPENAI_API_KEY=.*$/m, ""))
    assert.equal(env.OPENAI_API_KEY, undefined)
})
