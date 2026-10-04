#!/usr/bin/env python3
"""Paired first-write latency and sampled process-tree RSS, with durable restart checks."""

import argparse
import hashlib
import json
import os
import platform
import queue
import re
import signal
import statistics
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path


def main():
    args = arguments()
    metadata = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "baseline_sha256": hashlib.sha256(args.baseline.read_bytes()).hexdigest(),
        "candidate_sha256": hashlib.sha256(args.candidate.read_bytes()).hexdigest(),
        "baseline_sdk_host_sha256": hashlib.sha256(
            ((args.baseline_sdk or args.sdk) / "dist/host/actor-host.js").read_bytes()
        ).hexdigest(),
        "candidate_sdk_host_sha256": hashlib.sha256(
            (args.sdk / "dist/host/actor-host.js").read_bytes()
        ).hexdigest(),
        "runs": args.runs,
        "sizes": args.sizes,
        "sampler_interval_seconds": 0.005,
    }
    records = []
    for record in trials(args):
        records.append(record)
        print(json.dumps(record), flush=True)
        args.output.write_text(
            json.dumps(
                {
                    "metadata": metadata,
                    "records": records,
                    "summary": summarize(records),
                },
                indent=2,
            )
            + "\n"
        )


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--sdk", type=Path, required=True)
    parser.add_argument(
        "--baseline-sdk", type=Path,
        help="SDK matching the baseline executor protocol; defaults to --sdk",
    )
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--sizes", type=int, nargs="+", default=[0, 1048576])
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def trials(args):
    for size in args.sizes:
        for trial in range(args.runs):
            variants = (
                ["baseline", "candidate"]
                if trial % 2 == 0
                else ["candidate", "baseline"]
            )
            for variant in variants:
                sdk = (
                    (args.baseline_sdk or args.sdk).resolve()
                    if variant == "baseline" else args.sdk.resolve()
                )
                with tempfile.TemporaryDirectory(
                    prefix="terse-cold-write-"
                ) as directory:
                    project = Path(directory)
                    write_project(project, sdk)
                    for restored in [False, True]:
                        result = measure(
                            getattr(args, variant).resolve(),
                            sdk,
                            project,
                            size,
                            restored,
                        )
                        yield dict(
                            variant=variant,
                            trial=trial,
                            bytes=size,
                            restored=restored,
                            **result,
                        )


def write_project(project, sdk):
    (project / "actors.ts").write_text(
        "import { Actor, Persisted } from "
        + json.dumps(str(sdk / "dist/index.js"))
        + """;
export class Counter extends Actor {
    @Persisted count = 0;
    async write(bytes: number): Promise<number> {
        this.db.exec("CREATE TABLE IF NOT EXISTS payload (id INTEGER PRIMARY KEY, data BLOB)");
        if (!this.count) this.db.exec("INSERT INTO payload VALUES (1, zeroblob(?))", bytes);
        const size = this.db.exec<{size: number}>("SELECT length(data) AS size FROM payload WHERE id=1")[0]!.size;
        if (size !== bytes) throw new Error("payload was not restored");
        return ++this.count;
    }
}
"""
    )
    (project / "tsconfig.json").write_text(
        json.dumps(
            {
                "compilerOptions": {
                    "target": "ES2022",
                    "module": "NodeNext",
                    "moduleResolution": "NodeNext",
                    "strict": True,
                    "skipLibCheck": True,
                    "types": ["node"],
                    "typeRoots": [str(sdk / "node_modules/@types")],
                },
                "include": ["actors.ts"],
            }
        )
    )


def measure(binary, sdk, project, size, restored):
    env = dict(
        os.environ,
        DURABLE_ACTORS_PARENT_LIFETIME_STDIN="1",
        DURABLE_ACTORS_TELEMETRY="0",
        RUST_LOG="warn",
    )
    for name in [
        "DURABLE_ACTORS_PROJECT_ID",
        "DURABLE_ACTORS_SECRET",
        "DURABLE_ACTORS_API_KEY",
    ]:
        env.pop(name, None)
    with running(binary, sdk, project, env) as (runtime, origin):
        script = client_script(sdk)
        with subprocess.Popen(
            ["node", "--input-type=module", "--eval", script, origin, str(size)],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as client:
            result = invoke(client, runtime.pid, project)
            assert result.pop("count") == (2 if restored else 1)
            return result


def invoke(client, pid, project):
    assert client.stdout.readline().strip() == "prepared"
    sampler = Sampler(pid)
    sampler.start()
    client.stdin.write("go\n")
    client.stdin.flush()
    try:
        output, errors = client.communicate(timeout=60)
    finally:
        sampler.stop()
    if client.returncode != 0:
        raise RuntimeError(errors + (project / "runtime.log").read_text())
    return dict(**json.loads(output), **sampler.result())


@contextmanager
def running(binary, sdk, project, env):
    with (project / "runtime.log").open("w") as errors:
        runtime = subprocess.Popen(
            [
                str(binary),
                "dev",
                "--port",
                "0",
                "--entrypoint",
                "actors.ts",
                "--sdk-host",
                str(sdk / "dist/host.js"),
                "--project",
                str(project),
            ],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            start_new_session=True,
        )
        try:
            yield runtime, ready(runtime)
        finally:
            stop(runtime, project)


def stop(runtime, project):
    runtime.stdin.close()
    try:
        runtime.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(runtime.pid, signal.SIGTERM)
        runtime.wait(timeout=5)
    runtime.stdout.close()
    if runtime.returncode != 0:
        raise RuntimeError(
            f"runtime exited with {runtime.returncode}: "
            + (project / "runtime.log").read_text()
        )


def client_script(sdk):
    return (
        "import {createActorTransport} from "
        + json.dumps(str(sdk / "dist/backend.js"))
        + """;
const client = createActorTransport({controlPlaneUrl: process.argv[1]});
console.log('prepared');
await new Promise(resolve => process.stdin.once('data', resolve));
const start = performance.now();
const count = await client.invoke('Counter', 'one', 'write', [Number(process.argv[2])]);
console.log(JSON.stringify({latency_ms: performance.now() - start, count}));
process.exit(0);
"""
    )


def ready(runtime):
    lines = queue.Queue()

    def drain():
        for line in runtime.stdout:
            lines.put(line)
        lines.put(None)

    threading.Thread(target=drain, daemon=True).start()
    origin = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        line = lines.get(timeout=max(0.1, deadline - time.monotonic()))
        if line is None:
            raise RuntimeError("runtime exited before readiness")
        line = re.sub(r"\x1b\[[0-9;]*m", "", line)
        if "  Ready  " in line:
            origin = line.split("  Ready  ")[1].strip()
        if "durable-actors generate" in line and origin:
            return origin
    raise TimeoutError("runtime readiness")


class Sampler:
    def __init__(self, pid):
        self.pid = pid
        self.samples = []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.collect)

    def start(self):
        self.sample()
        self.thread.start()

    def stop(self):
        self.done.set()
        self.thread.join()
        self.sample()

    def collect(self):
        while not self.done.wait(0.005):
            self.sample()

    def sample(self):
        rows = [
            tuple(map(int, line.split()))
            for line in subprocess.check_output(
                ["ps", "-axo", "pid=,ppid=,rss="], text=True
            ).splitlines()
        ]
        descendants = {self.pid}
        while True:
            updated = descendants | {
                pid for pid, parent, _ in rows if parent in descendants
            }
            if updated == descendants:
                break
            descendants = updated
        self.samples.append(
            (
                time.monotonic(),
                sum(rss for pid, _, rss in rows if pid in descendants),
                len(descendants),
            )
        )

    def result(self):
        return {
            "idle_rss_kib": self.samples[0][1],
            "peak_rss_kib": max(row[1] for row in self.samples),
            "final_rss_kib": self.samples[-1][1],
            "peak_processes": max(row[2] for row in self.samples),
            "rss_samples": len(self.samples),
        }


def summarize(records):
    result = []
    for size, restored, variant in sorted(
        {(r["bytes"], r["restored"], r["variant"]) for r in records}
    ):
        rows = [
            r
            for r in records
            if (r["bytes"], r["restored"], r["variant"]) == (size, restored, variant)
        ]
        fields = [
            "latency_ms",
            "idle_rss_kib",
            "peak_rss_kib",
            "final_rss_kib",
            "peak_processes",
        ]
        result.append(
            dict(
                bytes=size,
                restored=restored,
                variant=variant,
                n=len(rows),
                **{
                    field: statistics.median(r[field] for r in rows) for field in fields
                },
            )
        )
    return result


if __name__ == "__main__":
    main()
