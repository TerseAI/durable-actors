"""One source build per sandbox; storage credentials are scoped by the control plane."""

import base64
import hashlib
import hmac
import io
import json
import os
import re
import signal
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

MAX_ARCHIVE = 128 * 1024 * 1024
MAX_EXPANDED = 1024 * 1024 * 1024
MAX_FILES = 20_000
MAX_CACHE_FILES = 100_000
COMPILER = "/opt/durable-actors/sdk/dist/compiler/deployment-build.js"


class BuildGate:
    def __init__(self, token):
        self.token = "Bearer " + token
        self.used = False
        self.lock = threading.Lock()

    def claim(self, authorization):
        with self.lock:
            if self.used or not hmac.compare_digest(authorization, self.token):
                return False
            self.used = True
            return True


class ObjectStore:
    def __init__(self, token):
        self.token = token

    def get(self, bucket, name, generation=None):
        query = {"alt": "media"}
        if generation is not None:
            query["generation"] = str(generation)
        url = f"https://storage.googleapis.com/storage/v1/b/{quote(bucket, safe='')}/o/{quote(name, safe='')}?{urlencode(query)}"
        try:
            with urlopen(
                Request(url, headers={"Authorization": "Bearer " + self.token}),
                timeout=60,
            ) as response:
                data = response.read(MAX_ARCHIVE + 1)
        except HTTPError as error:
            if error.code == 404:
                return None
            raise RuntimeError(
                f"Source storage read failed (HTTP {error.code})"
            ) from None
        if len(data) > MAX_ARCHIVE:
            raise ValueError("Build object exceeds the archive size limit")
        return data

    def put(self, bucket, name, data, optional=False):
        if len(data) > MAX_ARCHIVE:
            raise ValueError("Build object exceeds the artifact size limit")
        query = urlencode(
            {"uploadType": "media", "name": name, "ifGenerationMatch": "0"}
        )
        url = f"https://storage.googleapis.com/upload/storage/v1/b/{quote(bucket, safe='')}/o?{query}"
        request = Request(
            url,
            data=data,
            method="POST",
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/octet-stream",
            },
        )
        try:
            with urlopen(request, timeout=60) as response:
                return json.loads(response.read(64 * 1024))
        except HTTPError as error:
            if optional and error.code == 412:
                return None
            raise RuntimeError(
                f"Build artifact upload failed (HTTP {error.code})"
            ) from None


class SourceBuilder:
    def __init__(self, store, run=None):
        self.store = store
        self.run = run or self.command
        self.deadline = time.monotonic() + 600
        self.timings = {}

    def build(self, request, root):
        source = request["source"]
        entrypoint = safe_path(source["entrypoint"])
        project, output, cache = (
            root / name for name in ("project", "output", "cache")
        )
        for folder in (project, output, cache):
            folder.mkdir()
        obj = source["object"]
        started = time.monotonic()
        contents = self.store.get(obj["bucket"], obj["name"], obj["generation"])
        if contents is None:
            raise ValueError(
                "Source archive expired or is missing; upload the project again"
            )
        extract_source(contents, source["sha256"], project)
        del contents
        if not (project / entrypoint).is_file():
            raise ValueError("Actor entrypoint is missing from the source archive")
        self.record("sourceMs", started)

        environment = {
            **os.environ,
            "HOME": str(root),
            "TMPDIR": str(root),
            "npm_config_cache": str(cache / "npm"),
            "PIP_CACHE_DIR": str(cache / "pip"),
            "XDG_CACHE_HOME": str(cache / "xdg"),
            "XDG_DATA_HOME": str(root / "data"),
            "XDG_STATE_HOME": str(root / "state"),
            "PNPM_HOME": str(root / "pnpm"),
        }
        environment.pop("DURABLE_ACTORS_BUILD_TOKEN", None)
        cache_object = request["dependencyPrefix"] + dependency_key(project) + ".tar.gz"
        started = time.monotonic()
        cached = self.store.get(request["bucket"], cache_object)
        if cached is not None:
            extract_cache(cached, cache)
        self.install(project, cache, environment, entrypoint)
        self.record("dependenciesMs", started)

        started = time.monotonic()
        contract = json.loads(
            self.run(
                ["bun", COMPILER, str(project), entrypoint, str(output)],
                project,
                environment,
            )
        )
        self.record("compileMs", started)
        started = time.monotonic()
        manifest = self.publish(request, output, entrypoint)
        if cached is None:
            self.save_cache(request["bucket"], cache_object, cache)
        self.record("publishMs", started)
        return {
            "manifest": manifest,
            "contract": contract,
            "timings": self.timings,
            "dependencyCacheHit": cached is not None,
        }

    def install(self, project, cache, environment, entrypoint):
        if not (project / "package.json").is_file():
            if entrypoint.endswith(".py") and (project / "pyproject.toml").is_file():
                return
            raise ValueError(
                "Source archive requires package.json or a Python pyproject.toml"
            )
        manager, version = package_manager(project)
        if manager == "pnpm":
            executable = Path(
                f"/opt/durable-actors/pnpm/{version}/node_modules/.bin/pnpm"
            )
            if not executable.is_file():
                tools = project.parent / "tools"
                self.run(
                    [
                        "npm",
                        "install",
                        "--prefix",
                        str(tools),
                        "--no-fund",
                        "--no-audit",
                        f"pnpm@{version}",
                    ],
                    project.parent,
                    {
                        **environment,
                        "npm_config_cache": str(project.parent / "tool-cache"),
                    },
                )
                executable = tools / "node_modules/.bin/pnpm"
            frozen = (
                "--frozen-lockfile"
                if (project / "pnpm-lock.yaml").is_file()
                else "--no-frozen-lockfile"
            )
            self.run(
                [
                    str(executable),
                    "install",
                    "--prod",
                    frozen,
                    "--config.confirmModulesPurge=false",
                    "--store-dir",
                    str(cache / "pnpm"),
                ],
                project,
                environment,
            )
        else:
            operation = "ci" if (project / "package-lock.json").is_file() else "install"
            self.run(
                ["npm", operation, "--omit=dev", "--no-fund", "--no-audit"],
                project,
                environment,
            )
        if not entrypoint.endswith(".py"):
            self.run(
                ["node", "--input-type=module", "-e", SDK_LINK], project, environment
            )

    def save_cache(self, bucket, name, root):
        files = [
            file
            for file in sorted(root.rglob("*"))
            if file.is_file() and not file.is_symlink()
        ]
        total = sum(file.stat().st_size for file in files)
        if len(files) > MAX_CACHE_FILES or total > MAX_EXPANDED:
            print(
                json.dumps(
                    {
                        "event": "actor_dependency_cache_skipped",
                        "files": len(files),
                        "expandedBytes": total,
                    }
                ),
                flush=True,
            )
            return
        buffer = io.BytesIO()
        with tarfile.open(
            fileobj=buffer, mode="w:gz", compresslevel=1, dereference=True
        ) as archive:
            for file in files:
                archive.add(file, arcname=str(file.relative_to(root)), recursive=False)
        if buffer.tell() <= MAX_ARCHIVE:
            self.store.put(bucket, name, buffer.getvalue(), optional=True)
        else:
            print(
                json.dumps(
                    {"event": "actor_dependency_cache_skipped", "bytes": buffer.tell()}
                ),
                flush=True,
            )

    def publish(self, request, output, entrypoint):
        files = []
        for path in artifact_files(output, entrypoint):
            data = path.read_bytes()
            name = path.relative_to(output).as_posix()
            obj = request["artifactPrefix"] + name
            uploaded = self.store.put(request["bucket"], obj, data)
            files.append(
                {
                    "path": name,
                    "object": obj,
                    "generation": int(uploaded["generation"]),
                    "sha256": base64.urlsafe_b64encode(hashlib.sha256(data).digest())
                    .decode()
                    .rstrip("="),
                }
            )
        return {"bucket": request["bucket"], "files": files}

    def command(self, command, cwd, environment):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Actor source build exceeded ten minutes")
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            process = subprocess.Popen(
                command,
                cwd=cwd,
                env=environment,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise TimeoutError("Actor source build exceeded ten minutes") from None
            if process.returncode:
                stderr.seek(0, io.SEEK_END)
                stderr.seek(max(0, stderr.tell() - 8192))
                raise RuntimeError(
                    f"Build command failed ({command[0]}): {stderr.read().decode(errors='replace')}"
                )
            stdout.seek(0)
            result = stdout.read(4 * 1024 * 1024 + 1)
            if len(result) > 4 * 1024 * 1024:
                raise ValueError("Build command output exceeds the contract size limit")
            return result

    def record(self, phase, started):
        self.timings[phase] = round((time.monotonic() - started) * 1000)
        print(
            json.dumps(
                {
                    "event": "actor_source_build_phase",
                    "phase": phase,
                    "durationMs": self.timings[phase],
                }
            ),
            flush=True,
        )


SDK_LINK = """import { existsSync, symlinkSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, relative, resolve } from "node:path";
const link = resolve("node_modules/durable-actors");
if (!existsSync(link)) {
    const sdk = createRequire(import.meta.resolve("terse-sdk")).resolve("durable-actors");
    symlinkSync(relative(dirname(link), dirname(dirname(sdk))), link, "dir");
}"""


def safe_path(name):
    path = PurePosixPath(name)
    if (
        not name
        or "\\" in name
        or "\0" in name
        or path.is_absolute()
        or ".." in path.parts
        or str(path) != name
    ):
        raise ValueError("Invalid source archive path")
    return name


def extract_source(contents, digest, destination):
    if len(contents) > MAX_ARCHIVE or hashlib.sha256(contents).hexdigest() != digest:
        raise ValueError("Source archive digest or size does not match the deployment")
    with zipfile.ZipFile(io.BytesIO(contents)) as archive:
        entries = archive.infolist()
        if (
            len(entries) > MAX_FILES
            or sum(entry.file_size for entry in entries) > MAX_EXPANDED
        ):
            raise ValueError("Source archive exceeds the expanded size limit")
        seen = set()
        for entry in entries:
            name = safe_path(entry.filename.rstrip("/"))
            mode = entry.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise ValueError("Source archive cannot contain symlinks")
            if "node_modules" in PurePosixPath(name).parts or name in seen:
                raise ValueError(
                    "Source archive contains dependencies or duplicate paths"
                )
            seen.add(name)
            path = destination / name
            if entry.is_dir():
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("xb") as target, archive.open(entry) as source:
                    copy_limited(source, target, entry.file_size)
                path.chmod(0o755 if mode & 0o111 else 0o644)


def extract_cache(contents, destination):
    with tarfile.open(fileobj=io.BytesIO(contents), mode="r:gz") as archive:
        total = 0
        for index, entry in enumerate(archive):
            total += entry.size
            if index >= MAX_CACHE_FILES or total > MAX_EXPANDED or not entry.isfile():
                raise ValueError("Invalid dependency cache archive")
            name = safe_path(entry.name)
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as output, archive.extractfile(entry) as source:
                copy_limited(source, output, entry.size)


def copy_limited(source, destination, expected):
    remaining = expected
    while chunk := source.read(min(64 * 1024, remaining + 1)):
        remaining -= len(chunk)
        if remaining < 0:
            raise ValueError("Archive entry exceeds its declared size")
        destination.write(chunk)
    if remaining:
        raise ValueError("Truncated archive entry")


def dependency_key(project):
    names = {
        "package.json",
        "package-lock.json",
        "npm-shrinkwrap.json",
        "pnpm-lock.yaml",
        "pnpm-workspace.yaml",
        ".npmrc",
        "pyproject.toml",
        "uv.lock",
        "poetry.lock",
        "requirements.txt",
    }
    digest = hashlib.sha256()
    for path in sorted(project.rglob("*")):
        if path.is_file() and path.name in names:
            name, data = (
                path.relative_to(project).as_posix().encode(),
                path.read_bytes(),
            )
            digest.update(
                len(name).to_bytes(8, "big")
                + name
                + len(data).to_bytes(8, "big")
                + data
            )
    return digest.hexdigest()


def package_manager(project):
    manifest = json.loads((project / "package.json").read_text())
    configured = manifest.get("packageManager", "")
    if (project / "pnpm-lock.yaml").is_file() or configured.startswith("pnpm@"):
        version = (
            configured[5:].split("+")[0]
            if configured.startswith("pnpm@")
            else "10.34.1"
        )
        if not re.fullmatch(r"\d+\.\d+\.\d+", version):
            raise ValueError("Actor builds require an exact pnpm version")
        return "pnpm", version
    return "npm", None


def artifact_files(root, entrypoint):
    expected = root / ("actors.pyz" if entrypoint.endswith(".py") else "actors.mjs")
    if not expected.is_file() or not expected.stat().st_size:
        raise ValueError("Compiled actor entrypoint is missing or empty")
    files = []
    total = 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("Compiled artifacts cannot contain symlinks")
        if path.is_file():
            total += path.stat().st_size
            files.append(path)
        elif not path.is_dir():
            raise ValueError("Compiled artifact must be a regular file")
    if len(files) > MAX_FILES or total > MAX_EXPANDED:
        raise ValueError("Compiled artifacts exceed the size limit")
    return files


def serve():
    gate = BuildGate(os.environ["DURABLE_ACTORS_BUILD_TOKEN"])

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.reply(200 if self.path == "/ready" and not gate.used else 409, {})

        def do_POST(self):
            if self.path != "/build" or not gate.claim(
                self.headers.get("Authorization", "")
            ):
                self.reply(409, {"error": "Build worker is unavailable"})
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 64 * 1024:
                    raise ValueError("Invalid build request size")
                request = json.loads(self.rfile.read(size))
                # Pod deletion reclaims the workspace after the reply, without delaying deployment.
                result = SourceBuilder(ObjectStore(request["accessToken"])).build(
                    request, Path(tempfile.mkdtemp(prefix="actor-source-"))
                )
                self.reply(200, result)
            except (
                ValueError,
                KeyError,
                OSError,
                RuntimeError,
                tarfile.TarError,
                zipfile.BadZipFile,
            ) as error:
                self.reply(400, {"error": str(error)[-8192:]})

        def reply(self, status, body):
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 7102), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    serve()
