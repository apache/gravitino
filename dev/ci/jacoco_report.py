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

"""Parse JaCoCo XML coverage reports and generate a markdown summary for PRs.

Produces output compatible with the madrapps/jacoco-report format:
  - Overall project coverage with pass/fail status
  - Changed files coverage summary
  - Per-module coverage breakdown
  - Per-file detail table (collapsible) with links to source

Usage:
    python3 jacoco_report.py \
        --base-ref main \
        --head-sha abc123 \
        --repo-url https://github.com/apache/gravitino \
        --min-overall 40 \
        --min-changed 60 \
        --output coverage-report.md
"""

import argparse
import glob
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
import urllib.parse


def parse_counters(element):
    """Extract coverage counters from a JaCoCo XML element."""
    counters = {}
    for counter in element.findall("counter"):
        ctype = counter.get("type")
        counters[ctype] = {
            "missed": int(counter.get("missed", 0)),
            "covered": int(counter.get("covered", 0)),
        }
    return counters


def merge_counters(target, source):
    """Merge source counters into target."""
    for ctype, vals in source.items():
        if ctype not in target:
            target[ctype] = {"missed": 0, "covered": 0}
        target[ctype]["missed"] += vals["missed"]
        target[ctype]["covered"] += vals["covered"]


def coverage_pct(counter):
    """Calculate coverage percentage from a counter dict."""
    total = counter["missed"] + counter["covered"]
    return round(counter["covered"] / total * 100, 2) if total > 0 else 0.0


def extract_module_name(xml_path):
    """Extract the Gradle module name from a JaCoCo XML report path.

    Examples:
        "core/build/reports/jacoco/test/jacocoTestReport.xml" -> "core"
        "catalogs/catalog-hive/build/..." -> "catalog-hive"
    """
    parts = xml_path.replace("\\", "/").split("/build/")
    if len(parts) >= 2:
        module_path = parts[0]
        return module_path.rsplit("/", 1)[-1] if "/" in module_path else module_path
    return "unknown"


def parse_reports(pattern):
    """Parse all JaCoCo XML reports matching the glob pattern.

    Returns:
        overall: aggregated counters across all modules
        modules: dict mapping module_name to its aggregated counters
        source_files: dict mapping "package/SourceFile.java" to its counters
    """
    xml_files = sorted(glob.glob(pattern, recursive=True))
    overall = {}
    modules = {}
    source_files = {}

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            module_name = extract_module_name(xml_file)

            report_counters = parse_counters(root)
            merge_counters(overall, report_counters)

            if module_name not in modules:
                modules[module_name] = {}
            merge_counters(modules[module_name], report_counters)

            for pkg in root.findall(".//package"):
                pkg_name = pkg.get("name", "")
                for sf in pkg.findall("sourcefile"):
                    sf_name = sf.get("name", "")
                    key = f"{pkg_name}/{sf_name}"
                    if key not in source_files:
                        source_files[key] = {"modules": set()}
                    source_files[key]["modules"].add(module_name)
                    merge_counters(source_files[key], parse_counters(sf))
        except ET.ParseError as e:
            print(f"Warning: Could not parse {xml_file}: {e}", file=sys.stderr)

    return overall, modules, source_files


def get_changed_java_files(base_ref):
    """Get list of changed Java main source files compared to base ref."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=ACMR",
             f"origin/{base_ref}", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        files = [f for f in result.stdout.strip().split("\n") if f]
        return [f for f in files
                if f.endswith(".java") and "/src/main/java/" in f]
    except subprocess.CalledProcessError as e:
        print(f"Warning: git diff failed: {e.stderr}", file=sys.stderr)
        return []


def java_path_to_jacoco_key(java_path):
    """Convert a Java source file path to JaCoCo package/file key.

    Example:
        "core/src/main/java/org/apache/gravitino/Metalake.java"
        -> "org/apache/gravitino/Metalake.java"
    """
    parts = java_path.split("/src/main/java/")
    if len(parts) == 2:
        return parts[1]
    return None


def java_path_to_module(java_path):
    """Extract module name from a Java source file path.

    Example:
        "core/src/main/java/..." -> "core"
        "catalogs/catalog-hive/src/main/java/..." -> "catalog-hive"
    """
    parts = java_path.split("/src/main/java/")
    if len(parts) == 2:
        module_path = parts[0]
        return module_path.rsplit("/", 1)[-1] if "/" in module_path else module_path
    return "unknown"


def make_file_link(java_path, head_sha, repo_url):
    """Create a GitHub link for a source file."""
    if not head_sha or not repo_url:
        return os.path.basename(java_path)
    encoded = urllib.parse.quote(java_path, safe="/")
    name = os.path.basename(java_path)
    return f"[{name}]({repo_url}/blob/{head_sha}/{encoded})"


def compute_delta(overall_counter, changed_counter):
    """Compute coverage delta: how much the changed files affect coverage.

    Delta = overall_coverage - base_coverage (coverage without changed files).
    A negative delta means the changed files have lower coverage than the rest.
    """
    if not changed_counter:
        return None
    ov = overall_counter or {"missed": 0, "covered": 0}
    ch = changed_counter or {"missed": 0, "covered": 0}
    ov_total = ov["covered"] + ov["missed"]
    ch_total = ch["covered"] + ch["missed"]
    base_total = ov_total - ch_total
    if base_total <= 0 or ov_total <= 0:
        return None
    base_pct = (ov["covered"] - ch["covered"]) / base_total * 100
    overall_pct = ov["covered"] / ov_total * 100
    delta = round(overall_pct - base_pct, 2)
    if delta == 0.0:
        return None
    return delta


def format_delta(delta):
    """Format delta as bold backtick string like madrapps/jacoco-report."""
    if delta is None:
        return ""
    sign = "+" if delta > 0 else ""
    return f" **`{sign}{delta}%`**"


def generate_report(overall, modules, source_files, changed_java_files,
                    min_overall, min_changed, pass_emoji, fail_emoji,
                    head_sha, repo_url):
    """Generate a markdown coverage report matching madrapps/jacoco-report."""
    lines = []

    overall_line = overall.get("LINE", {"missed": 0, "covered": 0})
    overall_line_pct = coverage_pct(overall_line)
    overall_emoji = pass_emoji if overall_line_pct >= min_overall else fail_emoji

    # Compute changed files coverage and group by module
    changed_total = {}
    changed_by_module = {}
    changed_file_rows = []
    for jf in changed_java_files:
        key = java_path_to_jacoco_key(jf)
        if key and key in source_files:
            counters = {k: v for k, v in source_files[key].items()
                        if k != "modules"}
            merge_counters(changed_total, counters)
            mod = java_path_to_module(jf)
            if mod not in changed_by_module:
                changed_by_module[mod] = {}
            merge_counters(changed_by_module[mod], counters)
            line_c = counters.get("LINE", {"missed": 0, "covered": 0})
            changed_file_rows.append({
                "file": jf,
                "module": mod,
                "line_pct": coverage_pct(line_c),
            })

    changed_line_pct = coverage_pct(
        changed_total.get("LINE", {"missed": 0, "covered": 0})
    )
    changed_emoji = (pass_emoji if changed_line_pct >= min_changed
                     else fail_emoji)

    # Overall delta
    overall_delta = compute_delta(
        overall_line, changed_total.get("LINE")
    )

    # Hidden marker for comment update detection
    lines.append("<!-- coverage-report -->")

    # Header table (same style as madrapps/jacoco-report)
    lines.append("### Code Coverage Report")
    delta_str = format_delta(overall_delta)
    lines.append(
        f"|Overall Project|{overall_line_pct}%{delta_str}|{overall_emoji}|")
    lines.append("|:-|:-|:-:|")
    if changed_file_rows:
        lines.append(
            f"|Files changed|{changed_line_pct}%|{changed_emoji}|")
    else:
        lines.append("|Files changed|No Java source files changed|-|")
    lines.append("<br>")
    lines.append("")

    # Module-level table
    if modules:
        lines.append("|Module|Coverage||")
        lines.append("|:-|:-|:-:|")
        for mod_name in sorted(modules.keys()):
            mod_counters = modules[mod_name]
            mod_line = mod_counters.get("LINE", {"missed": 0, "covered": 0})
            mod_pct = coverage_pct(mod_line)
            mod_emoji = pass_emoji if mod_pct >= min_overall else fail_emoji
            mod_delta = compute_delta(
                mod_line,
                changed_by_module.get(mod_name, {}).get("LINE"),
            )
            mod_delta_str = format_delta(mod_delta)
            lines.append(
                f"|{mod_name}|{mod_pct}%{mod_delta_str}|{mod_emoji}|")
        lines.append("")

    # Per-file detail (collapsible)
    if changed_file_rows:
        lines.append("<details>")
        lines.append("<summary>Files</summary>")
        lines.append("")
        lines.append("|Module|File|Coverage||")
        lines.append("|:-|:-|:-|:-:|")

        sorted_rows = sorted(
            changed_file_rows, key=lambda r: (r["module"], -r["line_pct"])
        )
        prev_module = None
        for row in sorted_rows:
            mod_col = row["module"] if row["module"] != prev_module else ""
            file_link = make_file_link(row["file"], head_sha, repo_url)
            file_emoji = (pass_emoji if row["line_pct"] >= min_changed
                          else fail_emoji)
            lines.append(
                f"|{mod_col}|{file_link}|{row['line_pct']}%|{file_emoji}|"
            )
            prev_module = row["module"]

        lines.append("")
        lines.append("</details>")

    return "\n".join(lines), overall_line_pct, changed_line_pct


def write_github_outputs(overall_pct, changed_pct):
    """Write outputs to GITHUB_OUTPUT file for subsequent workflow steps."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"has_reports=true\n")
            f.write(f"coverage-overall={overall_pct}\n")
            f.write(f"coverage-changed-files={changed_pct}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate JaCoCo coverage report for PRs"
    )
    parser.add_argument("--base-ref", default="main",
                        help="Base branch ref for diff")
    parser.add_argument(
        "--report-pattern",
        default="**/build/reports/jacoco/test/jacocoTestReport.xml",
        help="Glob pattern for JaCoCo XML reports",
    )
    parser.add_argument("--min-overall", type=float, default=40,
                        help="Minimum overall line coverage percentage")
    parser.add_argument("--min-changed", type=float, default=60,
                        help="Minimum changed files line coverage percentage")
    parser.add_argument("--output", default="coverage-report.md",
                        help="Output markdown file path")
    parser.add_argument("--pass-emoji", default=":green_circle:",
                        help="Emoji for passing coverage")
    parser.add_argument("--fail-emoji", default=":red_circle:",
                        help="Emoji for failing coverage")
    parser.add_argument("--head-sha", default="",
                        help="HEAD commit SHA for file links")
    parser.add_argument("--repo-url", default="",
                        help="Repository URL for file links")
    args = parser.parse_args()

    overall, modules, source_files = parse_reports(args.report_pattern)

    if not overall:
        print("No JaCoCo reports found.")
        output_file = os.environ.get("GITHUB_OUTPUT")
        if output_file:
            with open(output_file, "a") as f:
                f.write("has_reports=false\n")
        return

    changed_java_files = get_changed_java_files(args.base_ref)

    report, overall_pct, changed_pct = generate_report(
        overall, modules, source_files, changed_java_files,
        args.min_overall, args.min_changed,
        args.pass_emoji, args.fail_emoji,
        args.head_sha, args.repo_url,
    )

    with open(args.output, "w") as f:
        f.write(report)

    print(report)
    write_github_outputs(overall_pct, changed_pct)


if __name__ == "__main__":
    main()
