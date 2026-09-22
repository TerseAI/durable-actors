# Upgrading to Durable Actors

Durable Actors replaces Little Actors. The GitHub repository is now [TerseAI/durable-actors](https://github.com/TerseAI/durable-actors).

## Applications

Replace the `little-actors` dependency with `durable-actors` and update every import, including subpaths such as `durable-actors/backend`, `durable-actors/compiler`, and `durable-actors/dev`. If you embed the observer, replace `little-actors-observer` with `durable-actors-observer` and update its imports too.

Use `npx durable-actors` for CLI commands and regenerate clients with `npx durable-actors generate`. The native executables are `durable-actors` and `durable-actors-modal-go`; runtime downloads now use the renamed GitHub repository and cache under `~/.cache/durable-actors`.

New local projects store state in `.durable-actors/`. To reuse existing local state, stop the old runtime and move `.little-actors/` to `.durable-actors/` before restarting, provided the destination does not already exist. Alternatively, keep the directory in place and run `npx durable-actors dev --data-dir .little-actors`. Preserve any custom `--data-dir` setting. Old state directories remain gitignored and excluded from the example apps' Vite file serving.

The `DURABLE_OBJECT_*` environment variables, actor source filenames, network protocols, database schema, persisted bucket paths, migration lock, and replica signing identifiers retain their existing contracts. A project rename requires no bucket or database migration.

## Self-hosted deployments

Build matching runtime and customer images using `durable-actors` packages. The runtime image name is now `us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/durable-actors`; it contains the SDK at `/opt/durable-actors/sdk`. Update custom Dockerfiles, executable paths, log filters (`durable_actors`), and Go imports (`github.com/TerseAI/durable-actors/providers/modal-go`). The Rust crate is `durable-actors`, imported as `durable_actors`.

Existing ngrok domains and deployed images are external resources and retain their current names until explicitly replaced. Keep using your assigned `NGROK_DOMAIN` and `NGROK_CONFIG`, updating a config path if you move its local state directory.

## First release under the new name

npm and crates.io packages cannot be renamed in place. Publish the new names after merging this change; keep old published versions available. The release workflow publishes `durable-actors-observer` before `durable-actors`, after the matching native bundles and container images are available. Do not publish the SDK alone before those runtime assets exist.

Configure npm trusted publishing for both new packages with owner `TerseAI`, repository `durable-actors`, and workflow `release.yml`. Initial publication may require a maintainer login before trusted publishing can be configured. Check that Google Cloud workload identity conditions and registry permissions accept the renamed repository before running the release.

After the new packages and runtime assets are available, deprecate the old npm packages with a message pointing to their replacements. Old GitHub URLs redirect, but existing checkouts should update their remote:

```sh
git remote set-url origin git@github.com:TerseAI/durable-actors.git
```
