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

"""Summarize retained Syft/Grype evidence without gating image publication."""

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

SCRIPT = "dev/ci/container_image_report.py"
REFERENCE = re.compile(
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?(?:@sha256:[a-f0-9]{64})?"
)


def normalize(reference):
    """Validate an OCI reference and deduplicate Docker Hub shorthand aliases."""
    if not REFERENCE.fullmatch(reference):
        raise ValueError("unresolved or unsupported image reference")
    repository = reference.split("@", 1)[0]
    if ":" not in repository.rsplit("/", 1)[-1] and "@" not in reference:
        reference += ":latest"
    first = reference.split("/", 1)[0]
    if "/" not in reference or not (
        "." in first or ":" in first or first == "localhost"
    ):
        reference = "docker.io/" + reference
    if reference.startswith("docker.io/") and reference.count("/") == 1:
        reference = reference.replace("docker.io/", "docker.io/library/", 1)
    return reference


def markdown(value):
    return str(value).replace("|", "\\|").replace("`", "'").replace("\n", " ")


def warning(message):
    """Emit an annotation without changing the process exit status."""
    escaped = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print("::warning title=Container image advisory::" + escaped, file=sys.stderr)


def scan_summary(image, directory, sbom_outcome, scan_outcome):
    """Keep CVEs, unavailable scans and missing inventory strictly advisory."""
    lines = [
        "## Container image advisory",
        "",
        f"Requested image: `{markdown(image)}` (Linux amd64).",
        "",
    ]
    warnings = []
    if sbom_outcome != "success" or scan_outcome != "success":
        warnings.append(
            f"Incomplete scan: inventory={sbom_outcome}, vulnerability scan={scan_outcome}. No clean result is claimed."
        )
    try:
        sbom = json.loads((directory / "sbom.json").read_text())
        metadata = sbom["source"]["metadata"]
        if normalize(metadata["userInput"].removeprefix("registry:")) != normalize(
            image
        ):
            raise ValueError("SBOM image differs from requested reference")
        if metadata["architecture"] != "amd64":
            raise ValueError("SBOM is not for the Linux amd64 scan target")
        lines.append(
            f"Resolved image manifest: `{markdown(metadata['manifestDigest'])}`."
        )
        packages = sbom["artifacts"]
        if not packages:
            warnings.append(
                "No packages were inventoried; this is not proof of a clean image."
            )
        types = Counter(package["type"] for package in packages)
        lines += [
            f"Inventoried packages: {len(packages)}. Package types: {markdown(dict(sorted(types.items())))}.",
            "",
        ]
        if "rustfs/rustfs:" in image and not types["rust-crate"]:
            warnings.append(
                "RustFS has no discovered Rust package metadata; its embedded Rust dependency CVEs are not covered."
            )
        scan = json.loads((directory / "vulnerabilities.json").read_text())
        counts = Counter(
            match["vulnerability"]["severity"].lower() for match in scan["matches"]
        )
        lines += ["| Severity | Matches |", "| --- | ---: |"]
        for severity, count in sorted(counts.items()):
            lines.append(f"| {markdown(severity)} | {count} |")
        if scan["matches"]:
            warnings.append(
                f"{len(scan['matches'])} vulnerability matches; inspect package versions, fixes and applicability in the retained report."
            )
        elif packages and sbom_outcome == "success" and scan_outcome == "success":
            lines.append("No known vulnerability matches among inventoried packages.")
    except (OSError, ValueError, KeyError, TypeError) as error:
        warnings.append("Incomplete or invalid scan evidence: " + str(error))
    lines += [
        "",
        "Metadata-based scanners can miss opaque/static dependencies. A clean report is not complete dependency coverage.",
        "",
    ]
    for message in warnings:
        lines.append("- **Warning:** " + markdown(message))
        warning(image + ": " + message)
    lines += [
        "",
        "Advisory only. See dev/ci/container-image-advisories.md for triage and coverage limits.",
    ]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--sbom-outcome", default="success")
    parser.add_argument("--scan-outcome", default="success")
    args = parser.parse_args()
    print(
        scan_summary(args.image, args.directory, args.sbom_outcome, args.scan_outcome)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
