# Shared documents

A small Tiptap editor with Yjs for concurrent edits and durable actors for saved documents.

## Run it

```sh
npx little-actors init documents-example --template documents
cd documents-example
npm install
npx little-actors generate
export DURABLE_OBJECT_API_KEY=local-dev-key
npx little-actors dev
```

Wait for `Local actors ready`. In another terminal, from the same directory:

```sh
export DURABLE_OBJECT_API_KEY=local-dev-key
npm run dev
```

Open [the editor](http://127.0.0.1:3000) in two tabs. Type in Welcome, format some text, and edit from both tabs. Add another document and switch between them. Reload after changes arrive in the other tab to see the saved content.

If you already have this directory, start at `npm install`. No external service is needed; choose the same local API key for both processes.

## The code

- [src/durable-objects.ts](src/durable-objects.ts): a workspace actor stores the document list; one document actor per ID merges and saves Yjs state. `@Emittable` publishes committed changes.
- [src/backend.ts](src/backend.ts): issues WebSocket credentials using the generated proxy and local defaults.
- [src/collaboration.ts](src/collaboration.ts): connects Yjs to the generated client. It sends local updates, applies remote state without echoing it, and resyncs the open document on reconnect.
- [src/App.tsx](src/App.tsx) and [src/Editor.tsx](src/Editor.tsx): document navigation and Tiptap’s editor, formatting controls, and collaborative undo/redo.

Yjs updates are encoded as base64 strings to fit the SDK’s JSON protocol. The actor saves the Yjs state, preserving the information needed to merge concurrent edits. Clients receive a complete state update after each committed edit; this keeps the sample small rather than optimizing for large documents.

Editing pauses while disconnected. Changes already in the open document are merged again on reconnect; there is no browser storage for unsaved edits. Keep `.little-actors/` and restart both servers to restore saved documents.

This demo has one shared workspace and no authentication. In an application, authenticate the proxy route and authorize access to the requested workspace or document. Presence cursors, document deletion, and permissions UI are left out.
