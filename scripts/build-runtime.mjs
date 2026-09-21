import { spawn } from "node:child_process"
import { createHash } from "node:crypto"
import { copyFile, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

export class RuntimeBuilder {
    constructor({ root, platform = process.platform, arch = process.arch }, run = runCommand) {
        this.root = root
        this.platform = platform
        this.arch = arch
        this.run = run
    }

    async build() {
        const output = path.join(this.root, "dist-runtime")
        await mkdir(output, { recursive: true })
        const staging = await mkdtemp(path.join(output, ".build-"))
        try {
            await this.compile(staging)
            return await this.package(staging, output)
        } finally {
            await rm(staging, { recursive: true, force: true })
        }
    }

    async compile(staging) {
        const provider = path.join(this.root, "target/release/little-actors-modal-go")
        await this.run("cargo", ["build", "--locked", "--release", "--bin", "little-actors"], { cwd: this.root })
        await copyFile(path.join(this.root, "target/release/little-actors"), path.join(staging, "little-actors"))
        await this.run("go", ["build", "-mod=readonly", "-trimpath", "-ldflags=-s -w", "-o", provider, "."], {
            cwd: path.join(this.root, "providers/modal-go"),
            env: { ...process.env, CGO_ENABLED: "0" }
        })
        await copyFile(provider, path.join(staging, "little-actors-modal-go"))
    }

    async package(staging, output) {
        const name = `little-actors-${this.platform}-${this.arch}.tar.gz`
        const archive = path.join(staging, name)
        await this.run("tar", ["-czf", archive, "-C", staging, "little-actors", "little-actors-modal-go"])
        const checksum = createHash("sha256")
            .update(await readFile(archive))
            .digest("hex")
        const destination = path.join(output, name)
        await writeFile(`${destination}.sha256`, `${checksum}  ${name}\n`)
        await rename(archive, destination)
        return destination
    }
}

function runCommand(command, args, options) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, { ...options, stdio: "inherit" })
        child.once("error", reject)
        child.once("exit", code => (code === 0 ? resolve() : reject(new Error(`${command} exited with ${code}`))))
    })
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    console.log(await new RuntimeBuilder({ root: fileURLToPath(new URL("../", import.meta.url)) }).build())
}
