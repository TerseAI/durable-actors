# Python actors

Install Node.js 22+, Python 3.11+, and uv. Run `pnpm install`, `uv sync`, then `pnpm exec durable-actors dev`. The shared TypeScript CLI selects `.venv` and checks actor types before starting and reloading.

Edit `src/actors.py`. From your client application, install `little-actors[codegen]` and run `pnpm exec durable-actors generate` against the running server. Generated Python clients include typed methods and models. The CLI runs strict mypy after generation.

Set `DURABLE_ACTORS_PYTHON` to select another interpreter. `DURABLE_ACTORS_BINARY` selects a locally built runtime.
