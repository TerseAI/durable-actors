"""Python project tooling. Install with durable-actors[cli]."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from importlib.metadata import distribution, version
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from .client import Client
from .codegen import generate_client


def parser() -> argparse.ArgumentParser:
    program = argparse.ArgumentParser(prog="durable-actors")
    program.add_argument("-V", "--version", action="version", version=version("durable-actors"))
    commands = program.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="Create a Python actor project")
    init.add_argument("directory", type=Path)
    dev = commands.add_parser("dev", help="Run local actors and reload source changes")
    dev.add_argument("--no-watch", action="store_true")
    dev.add_argument("--port", type=int)
    dev.add_argument("--project", type=Path)
    dev.add_argument("--entrypoint")
    dev.add_argument("--data-dir", type=Path)
    dev.add_argument("--storage", choices=["local", "gcs"])
    generate = commands.add_parser("generate", help="Generate typed Python clients")
    generate.add_argument("entrypoint", nargs="?", help="Compile this source instead of the server")
    generate.add_argument("--out-dir", type=Path, default=Path("generated"))
    generate.add_argument("--control-plane-url")
    observe = commands.add_parser("observe", help="Open the actor observer")
    observe.add_argument("--no-open", action="store_true")
    observe.add_argument("--control-plane-url")
    commands.add_parser("start", help="Run the control plane using its environment settings")
    return program


def main() -> None:
    options = parser().parse_args()
    try:
        from dotenv import load_dotenv

        # Explicit process variables win; local settings override shared settings.
        load_dotenv(Path.cwd() / ".env.local")
        load_dotenv(Path.cwd() / ".env")
        if options.command == "init":
            initialize(options.directory)
        elif options.command == "dev":
            arguments = ["dev"]
            for flag in ("port", "project", "entrypoint", "data-dir", "storage"):
                value = getattr(options, flag.replace("-", "_"))
                if value is not None:
                    arguments.extend([f"--{flag}", str(value)])
            if not options.no_watch:
                arguments.append("--watch")
            execute_runtime(arguments)
        elif options.command == "start":
            execute_runtime([])
        elif options.command == "generate":
            generate(options.entrypoint, options.out_dir, options.control_plane_url)
        elif options.command == "observe":
            from .observer import observe

            observe(options.control_plane_url, open_browser=not options.no_open)
    except (ImportError, ModuleNotFoundError) as error:
        print(f"{error}. Install durable-actors[cli] in this Python environment.", file=sys.stderr)
        raise SystemExit(1) from error
    except subprocess.CalledProcessError as error:
        print(error.stderr or error.stdout or str(error), file=sys.stderr)
        raise SystemExit(1) from error
    except (ValueError, OSError, RuntimeError, httpx.HTTPError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
    except KeyboardInterrupt:
        raise SystemExit(130) from None


def native_executable() -> Path:
    override = os.environ.get("DURABLE_ACTORS_BINARY")
    if override:
        return Path(override).absolute()
    runtime = distribution("durable-actors-runtime")
    if runtime.version != version("durable-actors"):
        raise RuntimeError("durable-actors and durable-actors-runtime versions must match")
    for file in runtime.files or []:
        if file.name == "durable-actors-native":
            return Path(str(runtime.locate_file(file)))
    raise RuntimeError("Native runtime is missing. Reinstall durable-actors[cli].")


def execute_runtime(arguments: list[str]) -> None:
    executable = native_executable()
    environment = {**os.environ, "DURABLE_ACTORS_PYTHON": sys.executable}
    environment.setdefault("DURABLE_ACTORS_PROCESS_ROLE", "control_plane")
    if "DURABLE_ACTORS_SECRET" not in environment and "DURABLE_ACTORS_API_KEY" in environment:
        environment["DURABLE_ACTORS_SECRET"] = environment["DURABLE_ACTORS_API_KEY"]
    # Replace the launcher so the native runtime owns signals, hosts and reloads.
    os.execve(executable, [str(executable), *arguments], environment)


def initialize(directory: Path) -> None:
    directory = directory.absolute()
    directory.mkdir(parents=True)  # Refuse existing directories, including empty ones.
    try:
        shutil.copytree(
            Path(__file__).with_name("template"),
            directory,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        manifest = directory / "pyproject.toml"
        name = re.sub(r"[^a-z0-9]+", "-", directory.name.lower()).strip("-") or "actors"
        manifest.write_text(
            manifest.read_text()
            .replace("PROJECT_NAME", name)
            .replace("SDK_VERSION", version("durable-actors"))
        )
        (directory / "gitignore").rename(directory / ".gitignore")
    except BaseException:
        shutil.rmtree(directory)
        raise
    print(
        f"Created Python actors in {directory}.\n\n  cd {shlex_quote(str(directory))}\n  uv run durable-actors dev\n\nEdit src/actors.py. Generate clients with: uv run durable-actors generate"
    )


def shlex_quote(value: str) -> str:
    from shlex import quote

    return quote(value)


def generate(entrypoint: str | None, output: Path, origin: str | None) -> None:
    if entrypoint and origin:
        raise ValueError("--control-plane-url cannot be combined with a source entrypoint")
    if entrypoint:
        with TemporaryDirectory(prefix="durable-actors-build-") as temporary:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "durable_actors.build",
                    str(Path.cwd()),
                    entrypoint,
                    temporary,
                    "local",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            contract = json.loads(result.stdout)
    else:
        with Client(origin) as client:
            contract = client.get_contract()
    generate_client(contract, output.resolve())
    from .build import check_actor

    check_actor(Path.cwd(), str(output.resolve()))
    print(f"Generated {len(contract['actors'])} actor contract(s) in {output.resolve()}.")


if __name__ == "__main__":
    main()
