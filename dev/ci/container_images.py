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

"""Discover declared container images and report advisory scan evidence."""

import argparse
import hashlib
import json
import re
import shlex
import subprocess
import sys
from collections import Counter
from pathlib import Path

REFERENCE = re.compile(
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?(?:@sha256:[a-f0-9]{64})?"
)
SCRIPT = "dev/ci/container_images.py"
WORKFLOW = ".github/workflows/container-image-advisories.yml"
LIMITS = (
    "Static declarations and default values only; runtime overrides and computed references are not evaluated.",
    "Locally built images and implicit Testcontainers helper images are not scanned; tracked Dockerfile bases are.",
    "Scans cover registry Linux amd64 images, not running containers, other platforms or local build outputs.",
    "Version PR coverage is limited to Dependabot-supported manifests; source-code and script literals need manual updates.",
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


def resolve_defaults(value, arguments=None):
    """Resolve literal defaults without reading the environment or executing code."""
    arguments = arguments or {}
    for _ in range(20):

        def substitute(match):
            expression = match.group(1)
            default = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?::-|-)(.*)", expression)
            if default:
                return arguments.get(default.group(1), default.group(2))
            return arguments.get(expression, match.group(0))

        updated = re.sub(r"\$\{([^{}]+)\}", substitute, value)
        updated = re.sub(
            r"\$([A-Za-z_][A-Za-z0-9_]*)", lambda m: arguments.get(m[1], m[0]), updated
        )
        if updated == value:
            break
        value = updated
    return value


class Inventory:
    """Keep source locations and unresolved declarations alongside scan targets."""

    def __init__(self):
        self.images = {}
        self.unresolved = []

    def gap(self, path, line, value, reason):
        self.unresolved.append(
            {"path": path, "line": line, "value": value, "reason": reason}
        )

    def add(self, path, line, value, kind, arguments=None):
        resolved = resolve_defaults(value, arguments)
        try:
            reference = normalize(resolved)
        except ValueError:
            self.gap(
                path,
                line,
                value,
                "Dynamic or unsupported reference; no image was guessed",
            )
            return
        source = {"path": path, "line": line, "kind": kind, "declared": value}
        self.images.setdefault(reference, []).append(source)

    def result(self):
        return {
            "images": [
                {
                    "reference": ref,
                    "id": hashlib.sha256(ref.encode()).hexdigest()[:16],
                    "sources": sources,
                }
                for ref, sources in sorted(self.images.items())
            ],
            "unresolved": self.unresolved,
            "limitations": list(LIMITS),
        }


def dockerfile_images(path, text, inventory):
    """Find FROM/COPY sources, resolving global ARG defaults and stage aliases."""
    arguments, stages = {}, set()
    started = False
    for line, value in enumerate(text.splitlines(), 1):
        if not re.match(r"\s*(?:ARG|FROM|COPY)\s", value, re.I):
            continue
        tokens = shlex.split(value, comments=True)
        if not tokens:
            continue
        if tokens[0].upper() == "ARG" and not started and len(tokens) == 2:
            key, separator, default = tokens[1].partition("=")
            if separator:
                arguments[key] = resolve_defaults(default, arguments)
        if tokens[0].upper() == "COPY":
            for token in tokens[1:]:
                if token.startswith("--from="):
                    reference = resolve_defaults(
                        token.removeprefix("--from="), arguments
                    )
                    if not reference.isdigit() and reference.lower() not in stages:
                        inventory.add(path, line, reference, "Dockerfile COPY")
            continue
        if tokens[0].upper() != "FROM":
            continue
        started = True
        values = [token for token in tokens[1:] if not token.startswith("--")]
        if not values:
            inventory.gap(path, line, value, "Unrecognized FROM declaration")
            continue
        reference = resolve_defaults(values[0], arguments)
        if reference.lower() != "scratch" and reference.lower() not in stages:
            inventory.add(path, line, values[0], "Dockerfile", arguments)
        if len(values) == 3 and values[1].lower() == "as":
            stages.add(values[2].lower())


def yaml_images(path, text, inventory):
    """Read image scalars and Helm defaults with safe YAML nodes and source marks."""
    import yaml

    compose = bool(re.search(r"(?:docker-)?compose.*\.ya?ml$", path))
    kind = "Compose" if compose else "CI/Kubernetes/Helm"
    visited = set()

    def visit(node):
        if id(node) in visited:
            return
        visited.add(id(node))
        if isinstance(node, yaml.SequenceNode):
            for child in node.value:
                visit(child)
        if not isinstance(node, yaml.MappingNode):
            return
        fields = {
            key.value: value
            for key, value in node.value
            if isinstance(key, yaml.ScalarNode)
        }
        for key, value in fields.items():
            line = value.start_mark.line + 1
            if (
                key in ("image", "container")
                and isinstance(value, yaml.ScalarNode)
                and value.value
            ):
                if compose and "build" in fields:
                    inventory.gap(
                        path,
                        line,
                        value.value,
                        "Locally built Compose service; not a registry dependency",
                    )
                else:
                    inventory.add(path, line, value.value, kind)
            elif key == "image" and isinstance(value, yaml.MappingNode):
                image = {
                    k.value: v.value
                    for k, v in value.value
                    if isinstance(v, yaml.ScalarNode)
                }
                if "repository" in image:
                    reference = image["repository"]
                    if image.get("registry"):
                        reference = image["registry"] + "/" + reference
                    if image.get("digest"):
                        reference += "@" + image["digest"]
                    elif image.get("tag"):
                        reference += ":" + image["tag"]
                    else:
                        inventory.gap(
                            path,
                            line,
                            reference,
                            "Helm tag is computed from chart metadata or overrides",
                        )
                        continue
                    inventory.add(path, line, reference, "Helm default")
            elif (
                key == "uses"
                and isinstance(value, yaml.ScalarNode)
                and value.value.startswith("docker://")
            ):
                inventory.add(
                    path,
                    line,
                    value.value.removeprefix("docker://"),
                    "Container action",
                )
            visit(value)

    try:
        for document in yaml.compose_all(text, Loader=yaml.SafeLoader):
            visit(document)
    except yaml.YAMLError:
        # Unrendered Helm templates are not necessarily valid YAML. Retain literal
        # image lines and disclose expressions instead of evaluating templates.
        if not (path.startswith("dev/charts/") and "/templates/" in path):
            inventory.gap(
                path,
                1,
                "",
                "YAML could not be parsed; only literal image lines inspected",
            )
        for line, value in enumerate(text.splitlines(), 1):
            match = re.match(r"\s*image:\s*(.+)", value)
            if match:
                inventory.add(path, line, match[1].strip().strip("\"'"), kind)


def code_images(path, text, inventory):
    """Find image literals in fixture declarations, calls, enums and Gradle defaults."""
    # Consume quoted strings before comments, so Gradle glob strings such as
    # "**/*" cannot accidentally hide subsequent image declarations.
    tokens = r""""(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|//[^\n]*|/\*[\s\S]*?\*/"""
    text = re.sub(
        tokens,
        lambda m: (
            re.sub(r"[^\n]", " ", m[0]) if m[0].startswith(("//", "/*")) else m[0]
        ),
        text,
    )
    patterns = [
        r'\b(?:\w*IMAGE\w*|image|imageName)\s*=\s*"([^"\n]+)"',
        r'(?:DockerImageName\.parse|withImage|new\s+\w*Container(?:<[^>]*>)?)\s*\(\s*"([^"\n]+)"',
        r'"[^"\n]*IMAGE"\s*(?:,|to)\s*"([^"\n]+)"',
    ]
    if path.endswith("ImageName.java"):
        patterns.append(r'"([^"\n]+)"')
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            inventory.add(
                path, text.count("\n", 0, match.start()) + 1, match[1], "Java/Gradle"
            )
    for match in re.finditer(r'System\.getenv\("([^"\n]*IMAGE)"\)', text):
        inventory.gap(
            path,
            text.count("\n", 0, match.start()) + 1,
            match[1],
            "Runtime override; declared Gradle defaults are inventoried separately",
        )


def shell_images(path, text, inventory):
    """Recognize literal docker run/pull invocations without executing shell code."""
    value_options = {
        "--name",
        "--restart",
        "--net",
        "--network",
        "--cap-add",
        "--platform",
        "--user",
        "--entrypoint",
        "-e",
        "--env",
        "-v",
        "--volume",
        "-p",
        "--publish",
        "-w",
        "--workdir",
    }
    flags = {"-i", "-t", "-it", "-d", "--rm", "--privileged", "--init"}
    for line, value in enumerate(text.splitlines(), 1):
        if value.lstrip().startswith("#"):
            continue
        match = re.search(r"\bdocker\s+(?:container\s+)?(?:run|pull)\s+(.+)", value)
        if not match:
            continue
        tokens = shlex.split(match[1], comments=True)
        while tokens:
            token = tokens.pop(0)
            if token in value_options and tokens:
                tokens.pop(0)
            elif token in flags or (token.startswith("--") and "=" in token):
                continue
            elif token.startswith("-"):
                inventory.gap(
                    path,
                    line,
                    value.strip(),
                    "Unsupported docker CLI option; image position unknown",
                )
                break
            else:
                inventory.add(path, line, token, "Docker CLI")
                break


def discover(root, paths):
    """Inspect tracked dependency declarations, excluding documentation and mocks."""
    inventory = Inventory()
    for path in sorted(paths):
        if path in (SCRIPT, WORKFLOW) or path.startswith(
            ("docs/", "rfc/", "dev/ci/test_")
        ):
            continue
        if (
            path.startswith("dev/charts/")
            and "/tests/" in path
            and "/templates/tests/" not in path
        ):
            continue
        file = root / path
        if file.is_symlink() or not file.is_file():
            continue
        name = file.name.lower()
        if not (
            "dockerfile" in name
            or "containerfile" in name
            or file.suffix in (".yml", ".yaml", ".java", ".kts", ".sh")
        ):
            continue
        try:
            text = file.read_text(encoding="utf-8")
            if "dockerfile" in name or "containerfile" in name:
                dockerfile_images(path, text, inventory)
            elif file.suffix in (".yml", ".yaml"):
                if re.search(r"(?m)^\s*(?:image|container|uses):", text):
                    yaml_images(path, text, inventory)
                shell_images(path, text, inventory)
            elif file.suffix in (".java", ".kts"):
                code_images(path, text, inventory)
            elif file.suffix == ".sh":
                shell_images(path, text, inventory)
        except (ValueError, OSError, UnicodeError) as error:
            inventory.gap(path, 1, "", "Could not inspect file: " + str(error))
    return inventory.result()


def markdown(value):
    return str(value).replace("|", "\\|").replace("`", "'").replace("\n", " ")


def inventory_summary(inventory):
    lines = [
        "## Container image inventory",
        "",
        f"Discovered {len(inventory['images'])} unique registry references.",
        "",
        "| Image | Declarations |",
        "| --- | --- |",
    ]
    for image in inventory["images"]:
        locations = sorted(
            {f"{source['path']}:{source['line']}" for source in image["sources"]}
        )
        lines.append(
            f"| `{image['reference']}` | {', '.join(markdown(location) for location in locations)} |"
        )
    lines += ["", "### Coverage boundaries", ""] + [
        "- " + limit for limit in inventory["limitations"]
    ]
    if inventory["unresolved"]:
        lines += [
            "",
            "### Declarations requiring review",
            "",
            "| Location | Declaration | Reason |",
            "| --- | --- | --- |",
        ]
        for gap in inventory["unresolved"]:
            lines.append(
                f"| {markdown(gap['path'])}:{gap['line']} | {markdown(gap['value'])} | {markdown(gap['reason'])} |"
            )
    return "\n".join(lines) + "\n"


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
    commands = parser.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser("inventory")
    inventory.add_argument("--root", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--github-output", type=Path)
    report = commands.add_parser("report")
    report.add_argument("--image", required=True)
    report.add_argument("--directory", type=Path, required=True)
    report.add_argument("--sbom-outcome", default="success")
    report.add_argument("--scan-outcome", default="success")
    args = parser.parse_args()
    if args.command == "report":
        print(
            scan_summary(
                args.image, args.directory, args.sbom_outcome, args.scan_outcome
            )
        )
        return 0
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        files = (
            subprocess.check_output(["git", "ls-files", "-z"], cwd=args.root)
            .decode()
            .split("\0")
        )
        result = discover(args.root, filter(None, files))
    except (OSError, ValueError, ImportError, subprocess.SubprocessError) as error:
        result = {
            "images": [],
            "unresolved": [{"path": ".", "line": 1, "value": "", "reason": str(error)}],
            "limitations": list(LIMITS),
        }
    if result["unresolved"]:
        warning(
            f"{len(result['unresolved'])} declarations could not be fully resolved; see the inventory artifact."
        )
    if not result["images"]:
        warning(
            "No registry images discovered; do not interpret this as complete coverage."
        )
    matrix = [
        {"reference": image["reference"], "id": image["id"]}
        for image in result["images"]
    ]
    if len(matrix) > 256:
        warning(
            "Inventory exceeds GitHub's 256-job matrix limit; split the scan before running. Full inventory retained."
        )
        matrix = []
    (args.output / "inventory.json").write_text(json.dumps(result, indent=2) + "\n")
    (args.output / "summary.md").write_text(inventory_summary(result))
    if args.github_output:
        with args.github_output.open("a") as output:
            output.write(
                "matrix="
                + json.dumps({"include": matrix})
                + "\ncount="
                + str(len(matrix))
                + "\n"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
