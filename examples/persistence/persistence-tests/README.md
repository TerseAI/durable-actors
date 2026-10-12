# Persistence test actor

The actor project for the [persistence example pair](../README.md). Start it with `bun run dev`; the sibling [CLI](../cli/README.md) calls it through a generated client.

[PersistenceTests](src/actors.ts) exercises immediate persisted writes, atomic rollback, interleaving, and recovery after worker crashes. `bun run check` checks its types. Local integration tests live in `tests/`.
