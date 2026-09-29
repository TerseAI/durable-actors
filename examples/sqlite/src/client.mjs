import { actors } from "../generated/index.js"

const actorId = process.env.NOTEBOOK_ID ?? "my-notebook"
const notebook = actors.Notebook.get(actorId)
const [command = "list", ...words] = process.argv.slice(2)

try {
    console.log(`Notebook: ${actorId}`)
    switch (command) {
        case "demo":
            await demo()
            break
        case "list":
            await list()
            break
        case "add":
            console.log("Added:", await notebook.add(words.join(" ")))
            await list()
            break
        case "rollback":
            await rollback()
            await list()
            break
        default:
            throw new Error('Usage: pnpm notes [list | add "your note" | rollback]')
    }
} catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
}

async function demo() {
    console.log("Saved state before this run:")
    await list()
    console.log("Added:", await notebook.add("Hello, SQLite! This note survives a restart."))
    console.log("Trying a write that throws after changing both SQL and @Persisted state:")
    await rollback()
    console.log("Saved state after this run (only the successful note was added):")
    await list()
    console.log("Restart pnpm dev, then run pnpm notes list to check persistence.")
}

async function list() {
    console.log(JSON.stringify(await notebook.list(), null, 2))
}

async function rollback() {
    try {
        await notebook.addThenFail("This note should never be saved")
    } catch (error) {
        if (!(error instanceof Error) || !error.message.includes("Intentional rollback")) throw error
        console.log("Expected failure:", error.message)
        return
    }
    throw new Error("The rollback example unexpectedly succeeded")
}
