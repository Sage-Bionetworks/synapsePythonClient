#!/usr/bin/env python3
"""Sync the version recorded in project metadata files to a single source of truth.

The canonical client version lives in ``synapseclient/synapsePythonClient``
(the ``latestVersion`` field). The ARPA-H BDF metadata files duplicate that
version, which can easily drift. This script keeps them in lockstep so the
version only has to be maintained in one place.

Files kept in sync:
  * bdf.yaml        -> top-level ``version:``
  * codemeta.json   -> ``"version"``
  * CITATION.cff    -> top-level ``version:``

Usage:
  python .github/scripts/sync_version_metadata.py            # report drift only
  python .github/scripts/sync_version_metadata.py --write    # rewrite to match
  python .github/scripts/sync_version_metadata.py --check    # exit 1 on drift
"""
import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
VERSION_FILE = REPO_ROOT / "synapseclient" / "synapsePythonClient"


def latest_version() -> str:
    """Return ``latestVersion`` from the canonical version file."""
    return json.loads(VERSION_FILE.read_text(encoding="utf-8"))["latestVersion"]


def _replace(path: Path, pattern: str, replacement: str, write: bool) -> bool:
    """Apply a single regex substitution. Return True if the file changed."""
    text = path.read_text(encoding="utf-8")
    new_text, count = re.subn(pattern, replacement, text, count=1)
    if count == 0:
        raise ValueError(f"Could not find a version field to update in {path.name}")
    changed = new_text != text
    if changed and write:
        path.write_text(new_text, encoding="utf-8")
    return changed


def sync(version: str, write: bool) -> list[str]:
    """Sync all metadata files to ``version``. Return the list that drifted."""
    targets = [
        # (path, regex, replacement) — each touches only the version value.
        (REPO_ROOT / "bdf.yaml", r"(?m)^version:[^\n]*$", f'version: "{version}"'),
        (
            REPO_ROOT / "codemeta.json",
            r'("version":\s*)"[^"]*"',
            rf'\1"{version}"',
        ),
        (REPO_ROOT / "CITATION.cff", r"(?m)^version:[^\n]*$", f'version: "{version}"'),
    ]
    drifted = []
    for path, pattern, replacement in targets:
        if not path.exists():
            continue
        if _replace(path, pattern, replacement, write):
            drifted.append(path.name)
    return drifted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write", action="store_true", help="Rewrite metadata files to match."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any file is out of sync (no changes made).",
    )
    args = parser.parse_args()

    version = latest_version()
    drifted = sync(version, write=args.write)

    if not drifted:
        print(f"Version metadata already in sync with {version}.")
        return 0

    if args.write:
        print(f"Updated {', '.join(drifted)} to version {version}.")
        return 0

    print(
        f"Version metadata out of sync with {version}: {', '.join(drifted)}.\n"
        "Run: python .github/scripts/sync_version_metadata.py --write"
    )
    return 1 if args.check else 0


if __name__ == "__main__":
    sys.exit(main())
