import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "cold_write", Path(__file__).with_name("cold_write.py")
)
cold_write = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cold_write)


class RuntimeLogTests(unittest.TestCase):
    def test_actor_failure_includes_the_runtime_stdout(self):
        class Runtime:
            stdout = io.StringIO(
                "  Ready  http://localhost:7100\ndurable-actors generate\nactor failed: disk full\n"
            )
            stdin = io.StringIO()
            returncode = 0

            def wait(self, timeout):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(cold_write.subprocess, "Popen", return_value=Runtime()),
                self.assertRaisesRegex(RuntimeError, "actor failed: disk full"),
                cold_write.running(root / "runtime", root / "sdk", root, {}),
            ):
                raise RuntimeError("actor host unavailable")


if __name__ == "__main__":
    unittest.main()
