"""Small event JSON-lines conversion tool for inspecting captured batches."""

import argparse
import json
import sys

from .core import ChangeEvent, ChangefeedError


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and normalize sqlite-changefeed JSON-lines events.")
    parser.add_argument("--pretty", action="store_true", help="pretty-print each normalized event")
    args = parser.parse_args()
    try:
        for line_number, line in enumerate(sys.stdin, 1):
            if not line.strip():
                continue
            event = ChangeEvent.from_json(line)
            output = json.dumps(event.to_dict(), ensure_ascii=False, sort_keys=True, indent=2 if args.pretty else None,
                                separators=None if args.pretty else (",", ":"), allow_nan=False)
            sys.stdout.write(output + "\n")
        return 0
    except (ChangefeedError, ValueError) as error:
        parser.exit(2, f"sqlite-changefeed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
