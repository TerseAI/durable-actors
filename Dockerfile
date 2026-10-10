FROM rust:1.91.0-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY .cargo ./.cargo
COPY migrations ./migrations
COPY proto ./proto
COPY src ./src
COPY third_party/terse-litestream ./third_party/terse-litestream
COPY docs/reference/openapi.yaml ./docs/reference/openapi.yaml
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --locked --release \
    && mkdir -p /out \
    && cp target/release/durable-actors /out/durable-actors

FROM oven/bun:1.4.2 AS sdk-builder
WORKDIR /build
COPY package.json bun.lock bunfig.toml ./
COPY sdk/package.json ./sdk/package.json
COPY sdk/bin ./sdk/bin
COPY packages/observer-ui/package.json ./packages/observer-ui/package.json
RUN bun install --frozen-lockfile --filter './sdk' --filter './packages/observer-ui'
COPY packages/observer-ui ./packages/observer-ui
COPY sdk/src ./sdk/src
COPY sdk/scripts/build-client-runtime.mjs ./sdk/scripts/build-client-runtime.mjs
COPY sdk/scripts/build-declarations.mjs ./sdk/scripts/build-declarations.mjs
COPY sdk/LICENSE.md ./sdk/LICENSE.md
COPY sdk/tsconfig*.json ./sdk/
RUN bun run --bun --cwd packages/observer-ui build \
    && bun run --bun --cwd sdk build:client \
    && bun run --bun --cwd sdk tsc -p tsconfig.build.json \
    && bun run --bun --cwd sdk build:declarations

FROM python:3.13-slim-bookworm AS python-sdk
WORKDIR /build
COPY sdk-python/pyproject.toml sdk-python/README.md sdk-python/LICENSE.md ./
COPY sdk-python/src ./src
RUN pip install --no-cache-dir .

FROM oven/bun:1.4.2 AS bun

FROM python:3.13-slim-bookworm

RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-sdk /usr/local /usr/local
COPY --from=builder /out/durable-actors /usr/local/bin/durable-actors
COPY third_party/terse-ltx/LICENSE /usr/share/licenses/terse-ltx/
COPY third_party/terse-litestream/LICENSE third_party/terse-litestream/NOTICE /usr/share/licenses/terse-litestream/
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
COPY --from=sdk-builder /build/node_modules /opt/durable-actors/node_modules
COPY --from=sdk-builder /build/sdk/node_modules /opt/durable-actors/sdk/node_modules
COPY --from=sdk-builder /build/packages/observer-ui /opt/durable-actors/packages/observer-ui
COPY --from=sdk-builder /build/sdk/dist /opt/durable-actors/sdk/dist
COPY --from=sdk-builder /build/sdk/bin /opt/durable-actors/sdk/bin
COPY sdk/package.json /opt/durable-actors/sdk/package.json
RUN mkdir -p /customer /node_modules \
    && ln -s /opt/durable-actors/sdk /node_modules/durable-actors
RUN useradd --uid 10000 --create-home --home-dir /home/runtime runtime
USER 10000:10000

ENV RUST_LOG=warn,durable_actors=info
ENV DURABLE_ACTORS_SDK_HOST=/opt/durable-actors/sdk/dist/host.js
ENTRYPOINT ["/usr/local/bin/durable-actors"]
