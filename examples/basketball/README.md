# Courtside

A single-player basketball practice tracker powered by a SQLite Durable Actor. The browser calls an Express API, which uses the generated client to invoke one `Practice` actor per session. Every stat is an event in that actor's own SQLite database; the actor calculates the analytics with SQL.

## Start locally

From the repository root, prepare dependencies and matching SDK/runtime builds if needed:

```sh
pnpm install --frozen-lockfile
pnpm --dir sdk build
cargo build --locked --bin durable-actors
cd examples/basketball
cp .env.example .env
```

Start the actors in one terminal:

```sh
pnpm dev:actors
```

Wait for `Ready`, then run from `examples/basketball` in another terminal:

```sh
pnpm dev
```

Open **http://127.0.0.1:3003**. The actor runtime uses port **7104**, project `basketball`, and its own `.durable-actors/` directory. The example pins Node 22.19.0 for pnpm commands and uses the local runtime binary from this checkout. It requires Bun 1.3.9+.

## Track a practice

- Tap **Made** or **Missed** for 2-pointers, 3-pointers, and free throws.
- Count rebounds, assists, steals, and turnovers with the extra stat buttons.
- **Undo last** removes the latest event and recalculates the session. Repeating an undo cannot remove earlier events.
- View points, field-goal percentage, shooting splits, effective field-goal percentage, the last ten attempts, and recent activity.
- **New practice** starts an independent actor. Use the session selector to return to recent practices on this browser, or bookmark a session's URL to reopen it elsewhere.

Field-goal percentage excludes free throws. Effective field-goal percentage is `(FG made + 0.5 × 3PT made) / FG attempted`; it can exceed 100%. The all-shots ring and last-ten strip include free throws. With no attempts, percentages display a dash.

Stats remain after page reloads and actor runtime restarts. Browser storage only remembers recent session links; the stats live in the Durable Actor. A failed connection disables new writes until you refresh the session, so you can see whether a previous tap was saved before repeating it.

## Work on the example

- `src/actors.ts`: SQLite event storage, undo, and analytics.
- `src/api.ts`: HTTP routes with an injected actor client.
- `src/App.tsx`: practice controls and analytics UI.
- `src/style.css`: responsive styling.

The UI and actor runtime watch source changes. Restart `pnpm dev` after changing backend routes or public actor methods; it also regenerates the client.

```sh
pnpm build
pnpm test
```

The integration test runs the real runtime on a free port with temporary data. It verifies shooting calculations, duplicate-event handling, undo, invalid-event rejection, session isolation, restart recovery, and further writes after recovery.
