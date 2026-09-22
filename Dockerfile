FROM golang:1.27.1-bookworm AS modal-builder

WORKDIR /build
COPY providers/modal-go/go.mod providers/modal-go/go.sum ./
RUN go mod download
COPY providers/modal-go/ ./
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/durable-actors-modal-go .

FROM rust:1.89.0-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY .cargo ./.cargo
COPY migrations ./migrations
COPY proto ./proto
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --locked --release \
    && mkdir -p /out \
    && cp target/release/durable-actors /out/durable-actors

FROM node:22.19.0-bookworm AS sdk-builder
WORKDIR /build
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY sdk/package.json ./sdk/package.json
COPY packages/observer-ui/package.json ./packages/observer-ui/package.json
COPY examples/chat/package.json ./examples/chat/package.json
COPY examples/ai-chat/package.json ./examples/ai-chat/package.json
COPY examples/documents/package.json ./examples/documents/package.json
RUN corepack enable && pnpm install --frozen-lockfile
COPY packages/observer-ui ./packages/observer-ui
COPY sdk/src ./sdk/src
COPY sdk/tsconfig*.json ./sdk/
COPY proto ./proto
RUN pnpm --dir packages/observer-ui build \
    && pnpm --dir sdk generate:proto \
    && pnpm --dir sdk exec tsc -p tsconfig.build.json \
    && cp proto/durable_object.proto sdk/dist/generated/durable_object.proto

FROM oven/bun:1.4.2 AS bun

FROM debian:bookworm-slim

RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/durable-actors /usr/local/bin/durable-actors
COPY --from=modal-builder /out/durable-actors-modal-go /usr/local/bin/durable-actors-modal-go
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
COPY --from=sdk-builder /build/node_modules /opt/durable-actors/node_modules
COPY --from=sdk-builder /build/sdk/node_modules /opt/durable-actors/sdk/node_modules
COPY --from=sdk-builder /build/packages/observer-ui /opt/durable-actors/packages/observer-ui
COPY --from=sdk-builder /build/sdk/dist /opt/durable-actors/sdk/dist
COPY sdk/package.json /opt/durable-actors/sdk/package.json
RUN mkdir -p /customer /node_modules \
    && ln -s /opt/durable-actors/sdk /node_modules/durable-actors

ENV RUST_LOG=warn,durable_actors=info
ENV DURABLE_ACTORS_SANDBOX_COMMAND=durable-actors-modal-go
ENV DURABLE_ACTORS_SDK_HOST=/opt/durable-actors/sdk/dist/host.js
ENTRYPOINT ["/usr/local/bin/durable-actors"]
