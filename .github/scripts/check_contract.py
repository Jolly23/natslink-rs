#!/usr/bin/env python3
"""Run the shared contract against an explicit local Go checkout and this crate."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("go_checkout", nargs="?", default=os.environ.get("GO_CONFORMANCE_DIR"))
    args = parser.parse_args()
    if not args.go_checkout:
        parser.error("set GO_CONFORMANCE_DIR or pass the Go checkout path")
    rust_root = Path(__file__).resolve().parents[2]
    go_root = Path(args.go_checkout).resolve()
    fixture = Path("testdata/contract-v1.json")
    try:
        if (rust_root / fixture).read_bytes() != (go_root / fixture).read_bytes():
            sys.exit("contract fixtures differ; update both implementations together")
        result = subprocess.run(
            [os.environ.get("GO", "go"), "test", "-json", "-short", "-run", "^TestSharedContract$", "-count=1", "."],
            cwd=go_root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        )
        events = [json.loads(line) for line in result.stdout.splitlines()]
        for event in events:
            if "Output" in event:
                print(event["Output"], end="", flush=True)
        if not any(event.get("Test") == "TestSharedContract" and event["Action"] == "pass" for event in events):
            sys.exit("Go contract test did not run")
        cargo = os.environ.get("CARGO", "cargo")
        listing = subprocess.run(
            [cargo, "test", "--locked", "--test", "contract", "--", "--list"],
            cwd=rust_root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout
        for required in ["shared_dedup_contract", "shared_url_redaction_contract"]:
            if f"{required}: test" not in listing.splitlines():
                sys.exit(f"Rust contract test is missing: {required}")
        subprocess.run([cargo, "test", "--locked", "--test", "contract"], cwd=rust_root, check=True)
        listing = subprocess.run(
            [cargo, "test", "--locked", "--lib", "shared_", "--", "--list"],
            cwd=rust_root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout
        for required in ["shared_subscriber_contract", "shared_auth_contract"]:
            if not any(line.endswith(f"::{required}: test") for line in listing.splitlines()):
                sys.exit(f"Rust contract test is missing: {required}")
        subprocess.run(
            [cargo, "test", "--locked", "--lib", "shared_"], cwd=rust_root, check=True
        )
    except (OSError, subprocess.CalledProcessError) as error:
        if isinstance(error, subprocess.CalledProcessError) and error.stdout:
            print(error.stdout, end="")
        sys.exit(f"contract check failed: {error}")
    print("Go/Rust shared contract passed with identical fixtures")


if __name__ == "__main__":
    main()
