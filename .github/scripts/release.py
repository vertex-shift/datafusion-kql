"""Validate crate identity, immutable release refs, and published checksums."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request

PACKAGE = "datafusion-kql-native"
REPOSITORY = "https://github.com/vertex-shift/datafusion-kql"
VERSION = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)


def git(root, *args):
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


def metadata(root):
    with (root / "Cargo.toml").open("rb") as manifest:
        manifest = tomllib.load(manifest)
    package = manifest["package"]
    version = package["version"]
    if package["name"] != PACKAGE:
        raise ValueError(f"release package must be {PACKAGE}")
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise ValueError("package version must be a canonical semantic version")
    prerelease = version.split("+", 1)[0].partition("-")[2]
    if any(part.isdigit() and len(part) > 1 and part.startswith("0") for part in prerelease.split(".")):
        raise ValueError("numeric prerelease identifiers must not have leading zeros")
    if manifest.get("lib", {}).get("name") != "datafusion_kql":
        raise ValueError("the public Rust library name must remain datafusion_kql")
    if package.get("publish") != ["crates-io"]:
        raise ValueError("publishing must be restricted to crates-io")
    if package.get("repository") != REPOSITORY or package.get("license") != "Apache-2.0":
        raise ValueError("unexpected package repository or license")
    filename = f"{PACKAGE}-{version}.crate"
    return {
        "package": PACKAGE,
        "version": version,
        "file": filename,
        "archive": f"target/package/{filename}",
        "sha": git(root, "rev-parse", "HEAD"),
    }


def validate_tag(root, info, tag):
    if tag != f"v{info['version']}":
        raise ValueError("release tag must exactly match v<the Cargo.toml version>")
    git(root, "check-ref-format", f"refs/tags/{tag}")
    commit = git(root, "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}")
    if commit != info["sha"]:
        raise ValueError("release checkout does not match the version tag")
    if git(root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("release checkout must be clean")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "refs/remotes/origin/main"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    if ancestor.returncode == 1:
        raise ValueError("release commit must already be merged into origin/main")
    ancestor.check_returncode()
    return {**info, "tag": tag}


def verify_published(root, info, attempts=6):
    archive = root / info["archive"]
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    request = urllib.request.Request(
        f"https://crates.io/api/v1/crates/{info['package']}/{info['version']}",
        headers={"User-Agent": f"{PACKAGE}-release ({REPOSITORY})"},
    )
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                version = json.load(response)["version"]
            if (
                version["crate"] != info["package"]
                or version["num"] != info["version"]
                or version["repository"] != REPOSITORY
                or version["checksum"] != checksum
            ):
                raise ValueError("published crate identity or checksum does not match the release")
            return
        except urllib.error.HTTPError as error:
            if error.code != 404 or attempt == attempts - 1:
                raise
            print("Waiting for the published version to become visible...", file=sys.stderr)
            time.sleep(10)
    raise ValueError("published version was not verified")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="require a matching version tag merged into main")
    parser.add_argument("--verify-published", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    try:
        info = metadata(root)
        if args.tag is not None:
            info = validate_tag(root, info, args.tag)
        if args.verify_published:
            verify_published(root, info)
        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a", encoding="utf-8") as output:
                for key, value in info.items():
                    output.write(f"{key}={value}\n")
        print(json.dumps(info, sort_keys=True))
    except (OSError, KeyError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Release validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
