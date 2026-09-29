import { Actor, Persisted } from "durable-actors"

type Note = { id: number; text: string }
type NotebookState = { edits: number; notes: Note[] }

export class Notebook extends Actor {
    @Persisted private edits = 0

    async list(): Promise<NotebookState> {
        this.#ensureTable()
        return {
            edits: this.edits,
            notes: this.db.exec<Note>("SELECT id, text FROM notes ORDER BY id")
        }
    }

    async add(text: string): Promise<Note> {
        return this.#insertNote(text)
    }

    async addThenFail(text: string): Promise<void> {
        this.#insertNote(text)
        throw new Error("Intentional rollback: neither the note nor the edit count should change")
    }

    #insertNote(text: string): Note {
        if (!text.trim()) throw new Error("Note text cannot be empty")
        this.#ensureTable()
        const [note] = this.db.exec<Note>("INSERT INTO notes (text) VALUES (?) RETURNING id, text", text)
        ++this.edits
        return note!
    }

    #ensureTable(): void {
        this.db.exec("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text TEXT NOT NULL)")
    }
}
