# Contributing to Durable Actors

We welcome bug reports, documentation improvements, examples, and code contributions. For a larger change, open an [issue](https://github.com/TerseAI/durable-actors/issues) to discuss the approach before starting. Small fixes can go straight to a pull request.

Please follow our [code of conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately through [SECURITY.md](SECURITY.md).

## Set up the repository

To use Durable Actors in your own application, start with the [language quickstarts](README.md#languages). To develop this repository, install:

- Bun 1.4.2 (the pinned runtime and package manager).
- Rust 1.91+ with Cargo and rustfmt, and a native C/C++ build toolchain.
- Helm 3 for changes to the Kubernetes chart.
- Python 3.11+ and uv for changes to `sdk-python`.
- PostgreSQL 16 for database tests. Docker is an optional way to run it.

Fork the repository, then clone your fork and install the workspace dependencies:

```sh
git clone https://github.com/YOUR_USERNAME/durable-actors.git
cd durable-actors
bun install --frozen-lockfile
bun run --bun --cwd sdk build
cargo build --locked
```

Repository builds set the pinned Google SDK’s Rapid append opt-in in `.cargo/config.toml`. When building outside the checkout (including `cargo install`), set `RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"`.

The SDK build includes the observer UI and actor template. A full native bundle can be built with `bun run --bun build`.

Cargo fetches the Rust `terse-litestream` and `terse-ltx` crates at the revisions pinned in `Cargo.toml` and `Cargo.lock`. No submodule checkout is needed. `third_party/` contains their license notices for distribution in native bundles and runtime images.

## Find your way around

| Directory               | Contents                                            |
| ----------------------- | --------------------------------------------------- |
| `src/`                  | Rust control plane, host, storage, and runtime      |
| `sdk-python/`           | Python actors, executor, and generated clients |
| `sdk/`                  | TypeScript SDK, compiler, generated client, and CLI |
| `packages/observer-ui/` | Actor observability UI                              |
| `charts/durable-actors/`          | GKE hosting with Standard GCS and optional Rapid          |
| `tests/`                | Rust tests and repository script tests              |
| `examples/`             | Chat, AI chat, and collaborative documents          |
| `docs/`                 | API and configuration references                    |

## Make a change

Keep changes focused and follow the [engineering conventions](AGENTS.md). For behavior changes, add a failing test first, implement the change, then refactor. Put tests and fixtures in the relevant project's `tests/` directory; CLI tests belong in `sdk/tests/cli/`. Rust unit tests use `#[path]` declarations to retain private access.

Update documentation and examples when changing a public API, configuration, or CLI behavior.

## Run checks

SQLite test fixtures use the upstream Go Litestream CLI to create and restore compatible backups. Install Go 1.27.1 and the pinned test tool, then add Go's binary directory to your PATH:

```sh
go install github.com/benbjohnson/litestream/cmd/litestream@v0.5.17
export PATH="$(go env GOPATH)/bin:$PATH"
```

The production runtime embeds the Rust crate and does not need this Go executable.

From the repository root:

```sh
bun run --bun format:check
bun run --bun test
bun run --bun docs:check
```

`bun run --bun test` covers the observer UI, repository scripts, Rust tests, and SDK. PostgreSQL tests skip their database work unless `DURABLE_ACTORS_TEST_POSTGRES_URL` is set. Use a dedicated test database; the suite creates and removes test schemas. For example, with Docker:

```sh
docker run --name durable-actors-test-postgres --rm -d \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=durable_actors_test \
  -p 127.0.0.1:5432:5432 postgres:16
docker exec durable-actors-test-postgres pg_isready -U postgres -d durable_actors_test
export DURABLE_ACTORS_TEST_POSTGRES_URL='postgresql://postgres:postgres@127.0.0.1:5432/durable_actors_test?sslmode=disable'
bun run --bun test
```

Wait until `pg_isready` reports that PostgreSQL is accepting connections before running the tests. Stop the disposable database with `docker stop durable-actors-test-postgres` when finished.

For runtime and integration changes, build the SDK and runtime, then run the opt-in tests with Bun on your PATH:

```sh
bun run --bun --cwd sdk build
cargo build --locked
cargo test --locked -- --ignored
```

For Kubernetes deployment changes:

```sh
helm lint charts/durable-actors -f charts/durable-actors/tests/values.yaml
bun test --timeout 60000 charts/durable-actors/tests/chart.test.mjs
```

For Python SDK changes, build the runtime and run from `sdk-python`:

```sh
uv sync --locked --all-extras
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src/durable_actors
uv run pyright
DURABLE_ACTORS_TEST_RUNTIME="$(cd .. && pwd)/target/debug/durable-actors" uv run pytest -q
uv build --no-sources
```

Python integration tests require `DURABLE_ACTORS_TEST_RUNTIME`; they skip without it. To run the shared CLI's Python tests from the repository root after building the SDK:

```sh
bun run --bun --cwd sdk tsc -p tsconfig.test.json
DURABLE_ACTORS_TEST_PYTHON="$PWD/sdk-python/.venv/bin/python" DURABLE_ACTORS_TEST_RUNTIME="$PWD/target/debug/durable-actors" bun test --timeout 60000 ./sdk/.test-dist/tests/cli/python.test.js
```
The release workflow requires a [PyPI trusted publisher](https://docs.pypi.org/trusted-publishers/adding-a-publisher/) for package `durable-actors`: owner `TerseAI`, repository `durable-actors`, workflow `release.yml`, environment `pypi`.

For SDK packaging or documentation changes, run the relevant checks:

```sh
bun run --bun --cwd sdk package:check
bun run --bun --cwd packages/observer-ui package:check
bun run --bun docs:build
```

The [CI workflow](.github/workflows/ci.yml) is the source of truth for toolchain versions and the full validation matrix.

The Dockerfile builds three production images from a shared Rust build. Validate a target with:

```sh
docker build --target typescript -t durable-actors:typescript .
bash scripts/test-runtime-image.sh durable-actors:typescript typescript
```

Repeat for `python` and `control-plane`. CI runs each target on amd64 and arm64. The TypeScript execution package bundles only the actor and host modules, collects dependency licenses, and is checked by `sdk package:check`. Examples, the CLI, compiler, and observer UI are excluded from that image. SQL migrations and `docs/reference/openapi.yaml` are Rust build inputs embedded in the executable; their source directories are not copied into the final images.

## Open a pull request

Describe the problem, the resulting behavior, and how you verified it. Link related issues and include screenshots for UI changes. Note any checks you could not run. Maintainers will review the change and arrange releases through the existing [release workflow](.github/workflows/release.yml).

Contributions are made under the repository's [MIT license](LICENSE.md).

Published chart packages include all three image digests from the same release. The release job uploads the chart to GitHub Releases and `ghcr.io/terseai/charts/durable-actors`. Configure that GHCR package for public access when first published.
