import json
import subprocess
import sys
import zipfile


def test_hosted_build_installs_dependencies_before_import_and_bundles_resources(tmp_path):
    dependency = tmp_path / "build_dep-1.0-py3-none-any.whl"
    with zipfile.ZipFile(dependency, "w") as wheel:
        wheel.writestr("build_dep/__init__.py", "initial_count = 7\n")
        wheel.writestr(
            "build_dep-1.0.dist-info/METADATA",
            "Metadata-Version: 2.1\nName: build-dep\nVersion: 1.0\n",
        )
        wheel.writestr(
            "build_dep-1.0.dist-info/WHEEL",
            "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        wheel.writestr("build_dep-1.0.dist-info/RECORD", "")
    project = tmp_path / "project"
    project.mkdir()
    (project / "data").mkdir()
    (project / "data/__init__.py").write_text("")
    (project / "data/message.txt").write_text("hello")
    (project / "actors.py").write_text("""from typing import Annotated
from importlib.resources import files
from build_dep import initial_count
from little_actors import Actor, Persisted
class Counter(Actor):
    count: Annotated[int, Persisted()] = initial_count
    async def read(self) -> str:
        return f"{self.count}:{files('data').joinpath('message.txt').read_text()}"
""")
    (project / "pyproject.toml").write_text(f"""[project]
name = "test-actors"
version = "0.1.0"
dependencies = ["build-dep @ {dependency.as_uri()}"]
[tool.little-actors]
include = ["data/*.txt"]
""")
    output = tmp_path / "output"
    result = subprocess.run(
        [sys.executable, "-m", "little_actors.build", str(project), "actors.py", str(output)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["actors"][0]["actorName"] == "Counter"
    loaded = subprocess.run(
        [
            sys.executable,
            "-c",
            "import asyncio,sys; from pathlib import Path; from little_actors.build import load_artifact; print(asyncio.run(load_artifact(Path(sys.argv[1]))[0]().read()))",
            str(output / "actors.pyz"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert loaded.returncode == 0, loaded.stderr
    assert loaded.stdout.strip() == "7:hello"


def test_contract_is_extracted_from_packaged_source_not_stale_bytecode(tmp_path):
    import os
    import py_compile

    source = tmp_path / "actors.py"
    text = """from typing import Literal
from little_actors import Actor
class Version(Actor):
    async def read(self) -> Literal["one"]:
        return "one"
"""
    source.write_text(text)
    before = source.stat()
    py_compile.compile(str(source), doraise=True)
    source.write_text(text.replace("one", "two"))
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "little_actors.build",
            str(tmp_path),
            "actors.py",
            str(tmp_path / "dist"),
            "local",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    schema = json.loads(result.stdout)["actors"][0]["rpc"]["schema"]
    assert schema["definitions"]["Method_read_Result"]["const"] == "two"
