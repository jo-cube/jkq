#!/usr/bin/env python3
"""Build an isolated opt-in jkq copy against a reviewed local jx checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("jx", type=Path)
parser.add_argument("destination", type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
jx = args.jx.resolve()
out = args.destination.resolve()
if not (jx / "crates/jx/Cargo.toml").is_file():
    parser.error("jx must contain crates/jx/Cargo.toml")
if not out.is_relative_to(root / "target") or out == root / "target":
    parser.error("destination must be a new directory inside jkq/target")
if out.exists():
    parser.error("destination already exists; choose a new directory")

paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).split(b"\0")
# Include retained, uncommitted changes without modifying either checkout.
paths += subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=root).split(b"\0")
source = {}
for path in sorted(set(paths) - {b""}):
    relative = Path(os.fsdecode(path))
    if relative.parts[0] not in {"src", "tests", "scripts", "docs"} and str(relative) not in {
        "Cargo.toml", "Cargo.lock", "Makefile", "rust-toolchain.toml", "README.md", "LICENSE", "AGENTS.md"
    }:
        continue
    original = root / relative
    if not original.is_file():
        continue
    target = out / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(original, target)
    source[str(relative)] = hashlib.sha256(original.read_bytes()).hexdigest()

manifest = out / "Cargo.toml"
text = manifest.read_text()
old = 'jx = { git = "https://github.com/jo-cube/jx.git", branch = "main" }'
if text.count(old) != 1:
    raise SystemExit("review the changed jx dependency boundary before building a trial")
manifest.write_text(text.replace(old, 'jx = { path = ' + json.dumps(str(jx / "crates/jx")) + ', features = ["experimental"] }'))
module = out / "src/transform/mod.rs"
text = module.read_text()
if text.count(".compile(source)") != 1:
    raise SystemExit("review the changed compilation boundary before building a trial")
module.write_text(text.replace(".compile(source)", ".compile_execution(source)"))
identity = {"jkq_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
            "jx_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=jx, text=True).strip(),
            "jx_status": subprocess.check_output(["git", "status", "--porcelain=v1"], cwd=jx, text=True),
            "source_sha256": source}
(out / "trial-source.json").write_text(json.dumps(identity, indent=2) + "\n")
env = dict(os.environ, CARGO_TARGET_DIR=str(out / "target"))
# Resolve only the local dependency replacement, then freeze its resulting lock.
for command in [["cargo", "check", "--offline", "--all-targets"], ["make", "check"], ["cargo", "build", "--release", "--offline", "--locked"]]:
    subprocess.run(command, cwd=out, env=env, check=True)
binary = out / "target/release/jkq"
identity["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
identity["lock_sha256"] = hashlib.sha256((out / "Cargo.lock").read_bytes()).hexdigest()
(out / "trial-source.json").write_text(json.dumps(identity, indent=2) + "\n")
print("Experimental trial only; the normal jkq build remains legacy:", binary)
