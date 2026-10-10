# Python actors

Install Python 3.11+, uv, and Bun 1.4.2+. Bun runs the shared CLI; actor code and clients run in Python.

From this project directory:

```sh
uv sync
bunx durable-actors dev
```

The CLI downloads the native runtime automatically, selects the project's `.venv`, and checks actor types before starting and reloading.

Edit `src/actors.py`. Write synchronous actor methods with ordinary `def`. Every instance field must use `persisted()` or `ephemeral()`. Use `emitted(persisted(...))` for state broadcasts and `ephemeral()` for temporary values such as caches and service clients. From your client application, install `durable-actors[codegen]` and run `bunx durable-actors generate` against the running server. Generated Python clients include synchronous typed methods and models. Import `actors` from the generated package and use `actors.Counter.get("one")`; the SDK manages the HTTP connection pool. Actors with emitted fields also expose `actor.subscribe(callback)` for typed live state; close the returned subscription when finished. The CLI runs strict mypy after generation.

Set `DURABLE_ACTORS_PYTHON` to select another interpreter. `DURABLE_ACTORS_BINARY` selects a locally built runtime.
