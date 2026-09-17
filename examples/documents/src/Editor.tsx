import { useEffect, useState } from "react"

import Collaboration from "@tiptap/extension-collaboration"
import { EditorContent, useEditor, useEditorState } from "@tiptap/react"
import StarterKit from "@tiptap/starter-kit"
import type { Doc } from "yjs"

import { openDocument } from "./collaboration.js"
import type { ConnectionStatus } from "./collaboration.js"

export function DocumentEditor({ id, title }: DocumentProps) {
    const [document, setDocument] = useState<Doc>()
    const [status, setStatus] = useState<ConnectionStatus>("connecting")

    useEffect(() => {
        const session = openDocument(id, status => {
            setStatus(status)
            if (status === "open") setDocument(session.document)
        })
        return session.close
    }, [id])

    return (
        <section aria-label={title}>
            <div className="document-heading">
                <h2>{title}</h2>
                <span role="status">{status === "open" ? "Connected" : status === "connecting" ? "Connecting…" : "Disconnected. Reload to retry."}</span>
            </div>
            {document ? <RichText document={document} connected={status === "open"} /> : <p>Loading document…</p>}
        </section>
    )
}

function RichText({ document, connected }: EditorProps) {
    const editor = useEditor({
        extensions: [StarterKit.configure({ undoRedo: false }), Collaboration.configure({ document })],
        editorProps: { attributes: { role: "textbox", "aria-label": "Document content", "aria-multiline": "true" } }
    })
    const active = useEditorState({
        editor,
        selector: ({ editor }) => ({ bold: editor?.isActive("bold"), italic: editor?.isActive("italic"), heading: editor?.isActive("heading"), list: editor?.isActive("bulletList") })
    })
    useEffect(() => {
        editor?.setEditable(connected)
    }, [editor, connected])

    return (
        <>
            <div className="formatting" role="group" aria-label="Formatting">
                <button disabled={!connected} aria-pressed={active?.bold} onClick={() => editor?.chain().focus().toggleBold().run()}>
                    Bold
                </button>
                <button disabled={!connected} aria-pressed={active?.italic} onClick={() => editor?.chain().focus().toggleItalic().run()}>
                    Italic
                </button>
                <button disabled={!connected} aria-pressed={active?.heading} onClick={() => editor?.chain().focus().toggleHeading({ level: 2 }).run()}>
                    Heading
                </button>
                <button disabled={!connected} aria-pressed={active?.list} onClick={() => editor?.chain().focus().toggleBulletList().run()}>
                    List
                </button>
            </div>
            <EditorContent editor={editor} />
        </>
    )
}

type DocumentProps = { id: string; title: string }
type EditorProps = { document: Doc; connected: boolean }
