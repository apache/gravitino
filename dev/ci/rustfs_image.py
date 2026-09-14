#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Validate the consumed RustFS image pin and summarize maintenance evidence."""

import argparse
import json
import re
import sys
from collections import Counter

VERSION = r"v?(\d+)\.(\d+)\.(\d+)(?:-(alpha|beta|rc)\.(\d+))?"
IMAGE = re.compile(r"rustfs/rustfs:(" + VERSION + r")@sha256:[a-f0-9]{64}")
TRIAGE = "See dev/ci/rustfs-image-maintenance.md for maintainer triage."


def image_version(image):
    """Require a numbered RustFS release and immutable manifest digest."""
    match = IMAGE.fullmatch(image)
    if not match:
        raise ValueError(
            "Expected rustfs/rustfs:<release>@sha256:<64 lowercase hex digits>"
        )
    return match.group(1)


def read_image(compose):
    """Read Docker Compose's normalized JSON, shared with the Java fixture."""
    image = compose["services"]["rustfs"]["image"]
    image_version(image)
    return image


def version_key(tag):
    """Order RustFS's numbered alpha/beta/rc tags before their stable release."""
    match = re.fullmatch(VERSION, tag)
    if not match:
        raise ValueError("Unsupported RustFS release tag: " + tag)
    major, minor, patch, stage, number = match.groups()
    return (
        int(major),
        int(minor),
        int(patch),
        {"alpha": 0, "beta": 1, "rc": 2, None: 3}[stage],
        int(number or 0),
    )


def newer_release(image, releases):
    """Find newer published releases, including the move from rc to stable."""
    current = version_key(image_version(image))
    candidates = []
    for release in releases:
        if release["draft"]:
            continue
        tag = release["tag_name"]
        if not re.fullmatch(VERSION, tag):
            continue
        version = version_key(tag)
        # Once stable, do not recommend moving back to a prerelease channel.
        if current[3] == 3 and (release["prerelease"] or version[3] != 3):
            continue
        candidates.append((version, tag))
    if not candidates:
        raise ValueError(
            "No supported published RustFS releases found; check the upstream response"
        )
    latest, tag = max(candidates)
    return tag if latest > current else None


def release_summary(image, releases):
    """Report update availability without editing the pin or creating issues."""
    tag = newer_release(image, releases)
    lines = ["## RustFS release check", "", f"Pinned image: `{image}`.", ""]
    if tag:
        lines += [
            f"Newer release: [{tag}](https://github.com/rustfs/rustfs/releases/tag/{tag}).",
            "Review its release notes and image manifest, then update the Compose pin in a PR.",
            "Require the affected integration tests before merging.",
        ]
    else:
        lines.append("No newer supported release found.")
    lines += ["", TRIAGE, ""]
    return "\n".join(lines), bool(tag)


def vulnerability_summary(image, sbom, scan):
    """Show the inventory boundary and flag high/critical matches, fixed or not."""
    image_version(image)
    source = sbom["source"]
    if (
        source["type"] != "image"
        or source["metadata"]["userInput"].removeprefix("registry:") != image
    ):
        raise ValueError("SBOM was not generated from the pinned image")
    if source["metadata"]["architecture"] != "amd64":
        raise ValueError("Expected an SBOM of the Linux amd64 CI image")
    packages = sbom["artifacts"]
    if not packages:
        raise ValueError(
            "SBOM contains no packages; this is not a clean vulnerability scan"
        )
    matches = scan["matches"]
    severities = Counter(
        match["vulnerability"]["severity"].lower() for match in matches
    )
    rust = [package for package in packages if package["type"] == "rust-crate"]
    auditable = [
        package
        for package in rust
        if package["foundBy"] == "cargo-auditable-binary-cataloger"
    ]
    lines = [
        "## RustFS vulnerability report",
        "",
        f"Image: `{image}` (Linux amd64).",
        "",
        f"Inventoried packages: {len(packages)}.",
        f"Rust packages: {len(rust)}; found in cargo-auditable binaries: {len(auditable)}.",
        "",
        "| Severity | Matches |",
        "| --- | ---: |",
    ]
    for severity in ("critical", "high", "medium", "low", "negligible", "unknown"):
        lines.append(f"| {severity} | {severities[severity]} |")
    if not auditable:
        lines += [
            "",
            "**Coverage gap:** no Rust packages were found in cargo-auditable binaries.",
            "A clean OS-package scan does not establish coverage of RustFS's embedded Rust dependencies.",
        ]
    lines += [
        "",
        "The retained SBOM records discovered packages, not proof that every dependency was detected.",
        "Consult RustFS security advisories as well as the raw vulnerability report.",
    ]
    needs_triage = bool(severities["high"] + severities["critical"])
    if needs_triage:
        lines += [
            "",
            "**Maintainer action required:** triage high/critical findings, including those without a fix.",
        ]
    lines += ["", TRIAGE, ""]
    return "\n".join(lines), needs_triage


def load_json(path):
    """Load a scanner/API report without additional Python dependencies."""
    with open(path, encoding="utf-8") as source:
        return json.load(source)


def main():
    """Return failure for invalid evidence, actionable vulnerabilities or updates."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "image", help="Read docker compose config --format json from stdin"
    )
    report = commands.add_parser("report", help="Summarize Syft and Grype JSON reports")
    report.add_argument("--image", required=True)
    report.add_argument("--sbom", required=True)
    report.add_argument("--scan", required=True)
    releases = commands.add_parser(
        "releases", help="Check the paginated upstream releases JSON"
    )
    releases.add_argument("--image", required=True)
    releases.add_argument("--releases", required=True)
    args = parser.parse_args()
    try:
        if args.command == "image":
            print(read_image(json.load(sys.stdin)))
            return 0
        if args.command == "report":
            summary, needs_triage = vulnerability_summary(
                args.image, load_json(args.sbom), load_json(args.scan)
            )
        else:
            summary, needs_triage = release_summary(
                args.image, load_json(args.releases)
            )
        print(summary)
        return int(needs_triage)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print("RustFS maintenance check failed: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
