"""Exercise the release worker with a real pnpm project and an in-memory object store."""

import hashlib
import importlib.util
import io
import json
import os
import tempfile
import zipfile
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "worker",
    os.environ.get("SOURCE_BUILD_WORKER", "/opt/durable-actors/source-build.py"),
)
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)
worker.COMPILER = os.environ.get("SOURCE_BUILD_COMPILER", worker.COMPILER)


class Store:
    def __init__(self):
        self.objects = {}

    def get(self, _bucket, name, generation=None):
        return self.objects.get(name)

    def put(self, _bucket, name, data, optional=False):
        self.objects[name] = data
        return {"generation": "1"}


store = Store()
for count in [1, 2]:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "package.json",
            json.dumps(
                {
                    "type": "module",
                    "packageManager": "pnpm@12.5.1",
                    "dependencies": {"terse-sdk": "0.8.4"},
                }
            ),
        )
        archive.writestr("pnpm-workspace.yaml", "allowBuilds:\n  esbuild: true\n")
        archive.writestr(
            "tsconfig.json",
            json.dumps(
                {
                    "compilerOptions": {
                        "target": "ES2022",
                        "module": "ESNext",
                        "moduleResolution": "bundler",
                        "strict": True,
                        "skipLibCheck": True,
                    }
                }
            ),
        )
        archive.writestr(
            "src/actor.ts",
            'import { Actor, Persisted, Emittable } from "terse-sdk";\n'
            + f"export class Counter extends Actor {{ @Persisted @Emittable count = {count}; async increment(by: number): Promise<number> {{ return this.count += by; }} }}",
        )
    data = output.getvalue()
    store.objects["source.zip"] = data
    directory = tempfile.TemporaryDirectory()
    result = worker.SourceBuilder(store).build(
        {
            "source": {
                "sha256": hashlib.sha256(data).hexdigest(),
                "entrypoint": "src/actor.ts",
                "object": {
                    "bucket": "sources",
                    "name": "source.zip",
                    "generation": "1",
                },
            },
            "bucket": "code",
            "artifactPrefix": f"artifacts/{count}/",
            "dependencyPrefix": "dependencies/project-runtime/",
        },
        Path(directory.name),
    )
    assert result["contract"]["actors"][0]["actorName"] == "Counter", result["contract"]
    assert result["dependencyCacheHit"] == (count == 2)
    assert len(store.objects[f"artifacts/{count}/actors.mjs"]) > 100
    directory.cleanup()
assert (
    store.objects["artifacts/1/actors.mjs"] != store.objects["artifacts/2/actors.mjs"]
)
print(
    "Source build smoke passed: pnpm SDK resolution, direct publication, and dependency cache reuse"
)

output = io.BytesIO()
with zipfile.ZipFile(output, "w") as archive:
    archive.writestr(
        "pyproject.toml",
        '[project]\nname = "counter"\nversion = "0.1.0"\ndependencies = ["durable-actors"]\n',
    )
    archive.writestr(
        "actors.py",
        "from durable_actors import Actor\nclass Counter(Actor):\n    def read(self) -> int:\n        return 42\n",
    )
data = output.getvalue()
store.objects["python.zip"] = data
with tempfile.TemporaryDirectory() as directory:
    result = worker.SourceBuilder(store).build(
        {
            "source": {
                "sha256": hashlib.sha256(data).hexdigest(),
                "entrypoint": "actors.py",
                "object": {
                    "bucket": "sources",
                    "name": "python.zip",
                    "generation": "1",
                },
            },
            "bucket": "code",
            "artifactPrefix": "artifacts/python/",
            "dependencyPrefix": "dependencies/python-project-runtime/",
        },
        Path(directory),
    )
assert result["contract"]["actors"][0]["actorName"] == "Counter", result["contract"]
assert len(store.objects["artifacts/python/actors.pyz"]) > 100
print("Source build smoke passed: Python actor compilation and direct publication")
