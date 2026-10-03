# WebSocket scalability benchmark

This benchmark measures one busy room at 128, 1,024, 8,192 and 32,768 connections with four load generators, two gateways and one active actor sandbox. It starts with a concurrent cold connection burst, then measures echo traffic, 25 complete-room broadcast rounds, and a 3,276-client reconnect burst. Counts, on-demand tagged lookup, observer inventory, metadata retention, and connection survival across a changed sandbox hostname are checked at each stage. At 128 connections it also pauses one reader for eight seconds while the actor queues 16 MiB to that socket, then checks ordered delivery after resuming. The configured actor idle timeout is one second; the test waits 20 seconds and sends gateway heartbeat requests before waking the actor.

Monitor the recorded actor pod names with Kubernetes during the idle window to confirm the old sandbox has terminated and heartbeat replies do not start another sandbox. A changed actor instance ID alone is insufficient evidence. The final result is emitted only if all phases pass; report the highest fully passing phase separately from partial admission.

## Run on GKE

Prerequisites: a matching Linux runtime image built from this checkout, Node 22+, pnpm, Python 3, OpenSSL, gcloud, kubectl, and access to the cluster and configured actor artifact bucket. Run commands from the repository root. Choose a unique benchmark name and confirm the context and reference deployment before applying anything.

```sh
export bench_context=gke_fluid-analogy-473415-c2_us-west4-a_terse-actors
export bench_namespace=terse-control
export bench_name=ws-scale-test
export bench_directory="$(mktemp -d /tmp/terse-ws-gke.XXXXXX)"
export bench_image='REGISTRY/durable-actors@sha256:YOUR_BUILD_DIGEST'
export bench_bucket='YOUR_CONFIGURED_ACTOR_CODE_BUCKET'

pnpm --dir sdk build
node tests/benchmarks/gke-build.mjs "$bench_directory"
kubectl --context "$bench_context" -n "$bench_namespace" get deployment actors-terse -o json > "$bench_directory/reference.json"
python3 tests/benchmarks/render-gke.py \
  --reference "$bench_directory/reference.json" --image "$bench_image" \
  --name "$bench_name" --namespace "$bench_namespace" --directory "$bench_directory"

export bench_object="$(node -e 'console.log(require(process.env.bench_directory + "/artifact.json").object)')"
gcloud storage cp "$bench_directory/actors.mjs" "gs://$bench_bucket/$bench_object"
export bench_generation="$(gcloud storage objects describe "gs://$bench_bucket/$bench_object" --format='value(generation)')"
node --input-type=module <<'JS'
import { readFile, writeFile } from 'node:fs/promises';
const directory = process.env.bench_directory;
const file = JSON.parse(await readFile(`${directory}/artifact.json`, 'utf8'));
file.generation = Number(process.env.bench_generation);
const contract = JSON.parse(await readFile(`${directory}/contract.json`, 'utf8'));
await writeFile(`${directory}/deployment.json`, JSON.stringify({
  bundle: { bucket: process.env.bench_bucket, files: [file] }, contract
}));
JS

kubectl --context "$bench_context" apply --server-side --field-manager=websocket-benchmark -f "$bench_directory/gke.json"
kubectl --context "$bench_context" -n "$bench_namespace" rollout status deployment/"$bench_name"
kubectl --context "$bench_context" -n "$bench_namespace" rollout status statefulset/"$bench_name-load"
kubectl --context "$bench_context" -n "$bench_namespace" exec -i "$bench_name-load-0" -- node --input-type=module -e '
let body = "";
for await (const chunk of process.stdin) body += chunk;
const response = await fetch(process.env.DURABLE_ACTORS_CONTROL_PLANE_URL + "/v1/projects/" + process.env.DURABLE_ACTORS_PROJECT_ID + "/deployment", {
  method: "PUT", headers: { authorization: "Bearer " + process.env.DURABLE_ACTORS_SECRET, "content-type": "application/json" }, body
});
console.log(response.status, await response.text());
if (!response.ok) process.exit(1);
' < "$bench_directory/deployment.json"

kubectl --context "$bench_context" -n "$bench_namespace" exec -i "$bench_name-load-0" -- node --input-type=module \
  < tests/benchmarks/gke-run.mjs | tee "$bench_directory/results.jsonl"
```

Check that deployment registration returns 200 before running the load test. Runtime image changes require registering the deployment again, because the deployment records its runtime image. Restart all four load pods between runs to clear counters and failure history. For diagnosis, `env BENCH_KEEP_OPEN=1 node --input-type=module` preserves sockets after the run; explicitly close or stop the load pods afterward.

Collect `kubectl top pods --containers` and container restart/OOM status for the benchmark control planes, generators, and actual actor pod during the run. `kubectl top` reports working-set memory, not RSS. Record image digests, actor pod resources, node/runtime details, and the exact test duration alongside results. The generated manifest contains test credentials and must not be committed.

The test uses small JSON messages and closed-loop senders/broadcast rounds. Broadcast latency includes coordinator HTTP calls and 10 ms polling. The TLS path is cluster-local nginx, not the public GKE ingress. The bounded slow-reader check does not establish indefinite slow-consumer tolerance. Results do not establish maximum-size payload throughput, multi-actor capacity, long soaks, or gateway-pod failure tolerance. A terminating actor sandbox is expected to preserve sockets; a terminating socket-owning gateway disconnects its clients.

Cleanup removes only the resources in the generated manifest, the recorded benchmark actor pods, and the uploaded test artifact:

```sh
kubectl --context "$bench_context" delete -f "$bench_directory/gke.json"
gcloud storage rm "gs://$bench_bucket/$bench_object"
```

Inspect the sandbox namespace and remove any remaining actor pods belonging to this run by their recorded names. Preserve results before deleting the temporary directory. Never delete sandbox pods using a shared production selector.

## Sustained busy-room measurement

The ramp deliberately uses a one-second idle timeout. Stop-and-wait broadcasts can therefore include another sandbox activation when delivering a round takes longer than that timeout. To measure a continuously active room separately, restart all four load pods, then run:

```sh
kubectl --context "$bench_context" -n "$bench_namespace" rollout restart statefulset/"$bench_name-load"
kubectl --context "$bench_context" -n "$bench_namespace" rollout status statefulset/"$bench_name-load"
kubectl --context "$bench_context" -n "$bench_namespace" exec -i "$bench_name-load-0" -- node --input-type=module \
  < tests/benchmarks/gke-busy.mjs | tee "$bench_directory/busy-results.jsonl"
```

This reconnects 32,768 clients and delivers 25 full-room broadcasts while one client sends application probes, pausing 100 ms after each reply. It verifies that the actor stays in one sandbox and that every broadcast reaches every client exactly once. Probes share the normal actor handler path; gateway automatic replies would not keep the actor active. Keep its resource measurements separate from the hibernation ramp.

## Local smoke test

```sh
pnpm --dir sdk build
cargo build --locked
node tests/benchmarks/websockets.mjs --connections 1024 --seconds 10 --rounds 50
```

Local mode starts and removes a temporary project. It is a correctness/development check, not GKE capacity evidence. `DURABLE_ACTORS_TEST_RUNTIME` can select a release binary. The local runner also supports `--url` for an already deployed benchmark project.

The harness uses the existing SDK and Node WebSocket client. k6 is not required for this workload.
