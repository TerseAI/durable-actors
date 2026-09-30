import hashlib
import importlib.util
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "source_build", ROOT / "scripts/source-build.py"
)
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


def archive(files):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as writer:
        for name, content in files.items():
            writer.writestr(name, content)
    return output.getvalue()


class SourceBuildTests(unittest.TestCase):
    def test_build_publishes_a_bundle_and_reuses_downloads_while_rerunning_install_scripts(
        self,
    ):
        class Store:
            def __init__(self):
                self.objects = {}
                self.reads = []

            def get(self, bucket, name, generation=None):
                self.reads.append((bucket, name, generation))
                return self.objects.get(name)

            def put(self, _bucket, name, data, optional=False):
                self.objects[name] = data
                return {"generation": "42"}

        storage = Store()
        calls = []

        def run(command, cwd, environment, payload=None):
            calls.append(command[0])
            self.assertNotIn("DURABLE_ACTORS_BUILD_TOKEN", environment)
            if command[0] == "bun":
                self.assertEqual(command, ["bun", build.COMPILER, "--stdin"])
                project, entrypoint, output = json.loads(payload)
                self.assertEqual(project, str(cwd))
                self.assertEqual(entrypoint, "src/actor.ts")
                (Path(output) / "actors.mjs").write_text(
                    (cwd / "src/actor.ts").read_text()
                )
                return b'{"version":1,"actors":[]}'
            return b""

        for count in range(2):
            contents = archive(
                {"package.json": "{}", "src/actor.ts": f"export const count = {count}"}
            )
            storage.objects["source.zip"] = contents
            request = {
                "source": {
                    "sha256": hashlib.sha256(contents).hexdigest(),
                    "entrypoint": "src/actor.ts",
                    "object": {
                        "bucket": "sources",
                        "name": "source.zip",
                        "generation": "7",
                    },
                },
                "bucket": "code",
                "artifactPrefix": f"artifacts/{count}/",
                "dependencyPrefix": "deps/project-runtime/",
            }
            with tempfile.TemporaryDirectory() as directory:
                reply = build.SourceBuilder(storage, run).build(
                    request, Path(directory)
                )
            self.assertEqual(reply["dependencyCacheHit"], count == 1)
            self.assertEqual(
                reply["manifest"]["files"][0]["object"], f"artifacts/{count}/actors.mjs"
            )
            self.assertEqual(reply["manifest"]["files"][0]["generation"], 42)
        self.assertEqual(calls.count("npm"), 2)
        self.assertIn(("sources", "source.zip", "7"), storage.reads)
        self.assertNotEqual(
            storage.objects["artifacts/0/actors.mjs"],
            storage.objects["artifacts/1/actors.mjs"],
        )

    def test_extracts_verified_source_and_preserves_executable_files(self):
        content = archive(
            {"package.json": "{}", "src/actor.ts": "export class Counter {}"}
        )
        with tempfile.TemporaryDirectory() as directory:
            build.extract_source(
                content, hashlib.sha256(content).hexdigest(), Path(directory)
            )
            self.assertEqual((Path(directory) / "package.json").read_text(), "{}")

    def test_rejects_wrong_digest_before_extracting(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "digest"):
                build.extract_source(
                    archive({"file": "data"}), "0" * 64, Path(directory)
                )
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_rejects_escaping_paths_and_uploaded_dependencies(self):
        for name in [
            "../outside",
            "/outside",
            "a/../../outside",
            "a\\outside",
            "node_modules/pkg/index.js",
        ]:
            content = archive({name: "data"})
            with (
                tempfile.TemporaryDirectory() as directory,
                self.subTest(name=name),
                self.assertRaises(ValueError),
            ):
                build.extract_source(
                    content, hashlib.sha256(content).hexdigest(), Path(directory)
                )

    def test_rejects_symlinks(self):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as writer:
            entry = zipfile.ZipInfo("link")
            entry.create_system = 3
            entry.external_attr = 0o120777 << 16
            writer.writestr(entry, "/etc/passwd")
        content = output.getvalue()
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaisesRegex(ValueError, "symlink"),
        ):
            build.extract_source(
                content, hashlib.sha256(content).hexdigest(), Path(directory)
            )

    def test_dependency_cache_reuses_code_edits_and_changes_with_lockfile_or_configuration(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "package.json").write_text('{"packageManager":"pnpm@12.5.1"}')
            (root / "pnpm-lock.yaml").write_text("lockfileVersion: 9")
            (root / "actor.ts").write_text("first")
            first = build.dependency_key(root)
            (root / "actor.ts").write_text("second")
            self.assertEqual(first, build.dependency_key(root))
            (root / "pnpm-lock.yaml").write_text("changed")
            self.assertNotEqual(first, build.dependency_key(root))
            second = build.dependency_key(root)
            (root / ".npmrc").write_text("registry=https://registry.example.com")
            self.assertNotEqual(second, build.dependency_key(root))

    def test_package_manager_requires_an_exact_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for version in ["pnpm@latest", "pnpm@10;touch /tmp/oops"]:
                (root / "package.json").write_text(
                    json.dumps({"packageManager": version})
                )
                with self.assertRaisesRegex(ValueError, "exact pnpm"):
                    build.package_manager(root)
            (root / "package.json").write_text('{"packageManager":"pnpm@12.5.1"}')
            self.assertEqual(build.package_manager(root), ("pnpm", "12.5.1"))

    def test_worker_accepts_only_one_authenticated_build(self):
        gate = build.BuildGate("secret")
        self.assertFalse(gate.claim("wrong"))
        self.assertTrue(gate.claim("Bearer secret"))
        self.assertFalse(gate.claim("Bearer secret"))

    def test_artifact_publication_rejects_links_and_empty_entrypoints(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "actors.mjs").write_text("")
            with self.assertRaisesRegex(ValueError, "empty"):
                build.artifact_files(root, "src/actor.ts")
            (root / "actors.mjs").write_text("export {}")
            (root / "escape").symlink_to("/etc/passwd")
            with self.assertRaisesRegex(ValueError, "symlink"):
                build.artifact_files(root, "src/actor.ts")


if __name__ == "__main__":
    unittest.main()
