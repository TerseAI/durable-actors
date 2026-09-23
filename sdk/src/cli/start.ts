import { Command } from "commander"

import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

export function registerStartCommand(program: Command): void {
    program
        .command("start")
        .description("Start the production control plane server")
        .action(async () => {
            const executable = await fetchRuntimeExecutablePath()
            process.exitCode = await startRustRuntime(executable, [], runtimeEnvironment(executable), true).exited
        })
}
