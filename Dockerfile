FROM golang:1.27.1-bookworm AS modal-builder

WORKDIR /build
COPY providers/modal-go/go.mod providers/modal-go/go.sum ./
RUN go mod download
COPY providers/modal-go/ ./
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/little-actors-modal-go .

FROM rust:1.89.0-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY .cargo ./.cargo
COPY migrations ./migrations
COPY proto ./proto
COPY src ./src
RUN cargo build --locked --release

FROM node:22.19.0-bookworm AS sdk-builder
WORKDIR /build
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY sdk/package.json ./sdk/package.json
COPY examples/chat/package.json ./examples/chat/package.json
COPY examples/ai-chat/package.json ./examples/ai-chat/package.json
COPY examples/documents/package.json ./examples/documents/package.json
RUN corepack enable && pnpm install --frozen-lockfile
COPY sdk/src ./sdk/src
COPY sdk/tsconfig*.json ./sdk/
COPY proto ./proto
RUN pnpm --dir sdk exec tsc -p tsconfig.build.json \
    && cp proto/durable_object.proto sdk/dist/generated/durable_object.proto

FROM oven/bun:1.4.2 AS bun

FROM debian:bookworm-slim

RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/little-actors /usr/local/bin/little-actors
COPY --from=modal-builder /out/little-actors-modal-go /usr/local/bin/little-actors-modal-go
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
COPY --from=sdk-builder /build/node_modules /opt/little-actors/node_modules
COPY --from=sdk-builder /build/sdk/node_modules /opt/little-actors/sdk/node_modules
COPY --from=sdk-builder /build/sdk/dist /opt/little-actors/sdk/dist
COPY sdk/package.json /opt/little-actors/sdk/package.json
RUN mkdir -p /customer /node_modules \
    && ln -s /opt/little-actors/sdk /node_modules/little-actors

ENV RUST_LOG=warn,little_actors=info
ENV DURABLE_OBJECT_SANDBOX_COMMAND=little-actors-modal-go
ENV DURABLE_OBJECT_SDK_HOST=/opt/little-actors/sdk/dist/host.js
ENTRYPOINT ["/usr/local/bin/little-actors"]
