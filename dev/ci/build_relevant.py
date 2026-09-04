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

"""Build-path ignore model matching the old build.yml pull_request filter.

On main, ``build`` skipped the whole workflow when every changed file was
under ``docs/assets/**`` or ``web-v2/**``. ``workflow_call`` cannot do that,
so Required CI always invokes build.yml. This module is the source of truth
for the inner ``build_relevant_changes`` output that restores the skip.

build.yml runs this file. Tests exercise it without contacting GitHub.
"""

import os
import shlex


# Same directories the old pull_request paths-ignore used. Keep trailing
# slashes so ``web-v2-extra/`` is not ignored.
IGNORED_PATH_PREFIXES = (
    "docs/assets/",
    "web-v2/",
)
IGNORED_PATHS = frozenset(prefix.rstrip("/") for prefix in IGNORED_PATH_PREFIXES)


def github_ignore_globs():
    """Return GitHub ``paths-ignore`` globs for the ignored prefixes."""
    return tuple(f"{prefix}**" for prefix in IGNORED_PATH_PREFIXES)


def normalize_path(path):
    """Normalize a repo-relative path for prefix matching.

    Only a literal ``./`` prefix is removed so ``.github`` and similar
    dot-directories stay intact. ``str.lstrip("./")`` would strip those.
    """
    normalized = (path or "").replace("\\", "/")
    if normalized.startswith("./"):
        return normalized[2:]
    return normalized


def is_ignored_path(path):
    """Return True when ``path`` is under the old pull_request ignore list."""
    normalized = normalize_path(path)
    if not normalized:
        return False
    if normalized in IGNORED_PATHS:
        return True
    return any(normalized.startswith(prefix) for prefix in IGNORED_PATH_PREFIXES)


def is_build_relevant(path):
    """Return True when a change at ``path`` should run compile-check or build."""
    return bool(normalize_path(path)) and not is_ignored_path(path)


def has_build_relevant_changes(paths):
    """Return True when any changed path is outside the old ignore list.

    An empty list is relevant (fail closed): a missing file list must not
    skip compile-check and build while Required CI stays green.
    """
    paths = list(paths)
    if not paths:
        return True
    return any(is_build_relevant(path) for path in paths)


def should_run_compile_check(build_relevant, source_changes):
    """Light assemble: relevant non-source PRs, never ignore-only PRs."""
    return bool(build_relevant) and not bool(source_changes)


def should_run_build(build_relevant, source_changes):
    """Full Gradle build: relevant source PRs, never ignore-only PRs."""
    return bool(build_relevant) and bool(source_changes)


def decide(paths, source_changes):
    """Return skip/run flags for the build.yml compile-check and build jobs."""
    relevant = has_build_relevant_changes(paths)
    return {
        "build_relevant_changes": relevant,
        "compile_check": should_run_compile_check(relevant, source_changes),
        "build": should_run_build(relevant, source_changes),
    }


def parse_changed_files(raw):
    """Parse dorny ``list-files: shell`` output into path strings."""
    return shlex.split(raw or "")


def write_github_output(relevant):
    """Write ``build_relevant_changes`` for later jobs in build.yml."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    value = "true" if relevant else "false"
    print(f"build_relevant_changes={value}")
    if output_file:
        with open(output_file, "a", encoding="utf-8") as handle:
            handle.write(f"build_relevant_changes={value}\n")


def main():
    """Entry point for build.yml. Read dorny's all-changes file list."""
    paths = parse_changed_files(os.environ.get("ALL_CHANGE_FILES", ""))
    relevant = has_build_relevant_changes(paths)
    write_github_output(relevant)


if __name__ == "__main__":
    main()
