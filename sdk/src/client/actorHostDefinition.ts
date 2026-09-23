import { loadPackageDefinition } from "@grpc/grpc-js"
import { loadSync } from "@grpc/proto-loader"
import { fileURLToPath } from "node:url"

import type { ProtoGrpcType } from "../generated/durable_actors.js"

const definition = loadPackageDefinition(
    loadSync(fileURLToPath(new URL("../generated/durable_actors.proto", import.meta.url)), {
        defaults: true,
        longs: Number,
        oneofs: true
    })
) as unknown as ProtoGrpcType
const ActorHostClient = definition.durable_actors.v1.ActorHostService

export { ActorHostClient }
