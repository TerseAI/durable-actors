"""Load optional CLI dependencies only when invoking the command."""

import sys


def main() -> None:
    try:
        from .cli import main as run
    except ImportError as error:
        print(f"{error}. Install durable-actors[cli] in this Python environment.", file=sys.stderr)
        raise SystemExit(1) from error
    run()


if __name__ == "__main__":
    main()
