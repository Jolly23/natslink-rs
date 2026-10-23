#!/usr/bin/env python3
"""Validate the crate's release and Go-reference metadata."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def version_parts(value: str, label: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value)
    if match is None:
        raise ValueError(f"{label} must be a three-component release version")
    return tuple(map(int, match.groups()))


def check(root: Path, tag: str | None = None) -> None:
    metadata = json.loads(subprocess.run(
        [os.environ.get("CARGO", "cargo"), "metadata", "--locked", "--no-deps", "--format-version=1"],
        cwd=root, check=True, text=True, stdout=subprocess.PIPE,
    ).stdout)
    package = next(p for p in metadata["packages"] if Path(p["manifest_path"]).resolve() == root / "Cargo.toml")
    reference = package["metadata"]["go-reference"]
    crate_version = version_parts(package["version"], "Cargo version")
    reference_version = version_parts(reference["version"], "Go reference version")
    if not reference["version"].startswith("v"):
        raise ValueError("Go reference version must be a v-prefixed release tag")
    if crate_version[:2] != reference_version[:2]:
        raise ValueError("crate and Go reference must have the same major/minor version")
    constant = re.search(
        r'^pub const GO_REFERENCE_VERSION: &str = "([^"]+)";',
        (root / "src/lib.rs").read_text(),
        re.MULTILINE,
    )
    if constant is None or constant.group(1) != reference["version"]:
        raise ValueError("GO_REFERENCE_VERSION must exactly match go-reference.version")
    if reference["module"] != "github.com/Jolly23/natslink":
        raise ValueError("go-reference.module must name the public Go implementation")
    if re.fullmatch(r"[0-9a-f]{40}", reference["revision"]) is None:
        raise ValueError("go-reference.revision must be a full commit ID")
    if re.fullmatch(r"h1:[A-Za-z0-9+/]{43}=", reference["checksum"]) is None:
        raise ValueError("go-reference.checksum must be a Go module h1 checksum")
    if tag is not None and (not tag.startswith("v") or version_parts(tag, "tag") != crate_version):
        raise ValueError("release tag must equal v<Cargo package version>")
    print(f"reference OK: Rust {package['version']}, Go {reference['version']} (patches independent)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="also validate a release tag")
    args = parser.parse_args()
    try:
        check(Path(__file__).resolve().parents[2], args.tag)
    except (KeyError, ValueError, OSError, StopIteration, subprocess.CalledProcessError) as error:
        sys.exit(f"reference check failed: {error}")


if __name__ == "__main__":
    main()
