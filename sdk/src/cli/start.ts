import { Command } from "commander"

import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

export function registerStartCommand(program: Command): void {
    program
        .command("start")
        .description("Start the packaged runtime using your self-hosting environment settings")
        .action(async () => {
            const executable = await fetchRuntimeExecutablePath()
            process.exitCode = await startRustRuntime(executable, [], runtimeEnvironment(executable), true).exited
        })
}
