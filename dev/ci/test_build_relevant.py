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

"""Unit tests for the build.yml docs-assets / web-v2 ignore model.

These tests do not contact GitHub. They lock the old pull_request
``paths-ignore`` semantics Yuqi asked to restore on PR 12545.
"""

import sys
from pathlib import Path


CI_DIR = Path(__file__).resolve().parent
REPO_ROOT = CI_DIR.parents[1]
if str(CI_DIR) not in sys.path:
    sys.path.insert(0, str(CI_DIR))

import build_relevant


def check_equal(name, actual, expected):
    """Raise a readable error when a value differs from the expected one."""
    if actual != expected:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def test_docs_assets_only_is_a_noop():
    """A docs-assets-only PR must skip assemble, not run compile-check."""
    result = build_relevant.decide(["docs/assets/logo.png"], source_changes=False)
    check_equal("docs-assets relevant", result["build_relevant_changes"], False)
    check_equal("docs-assets compile-check", result["compile_check"], False)
    check_equal("docs-assets build", result["build"], False)


def test_web_v2_src_skips_even_when_dorny_calls_it_source():
    """``**/src/**`` must not pull web-v2 into the full Gradle build."""
    result = build_relevant.decide(
        ["web-v2/web/src/App.tsx"],
        source_changes=True,
    )
    check_equal("web-v2 relevant", result["build_relevant_changes"], False)
    check_equal("web-v2 compile-check", result["compile_check"], False)
    check_equal("web-v2 build", result["build"], False)


def test_mixed_web_v2_and_java_still_builds():
    """Old paths-ignore skipped only when every file was ignored."""
    result = build_relevant.decide(
        ["web-v2/web/src/App.tsx", "core/src/main/java/Foo.java"],
        source_changes=True,
    )
    check_equal("mixed relevant", result["build_relevant_changes"], True)
    check_equal("mixed compile-check", result["compile_check"], False)
    check_equal("mixed build", result["build"], True)


def test_non_source_relevant_file_runs_compile_check():
    """LICENSE-only still gets the light assemble; it was never paths-ignored."""
    result = build_relevant.decide(["LICENSE"], source_changes=False)
    check_equal("license relevant", result["build_relevant_changes"], True)
    check_equal("license compile-check", result["compile_check"], True)
    check_equal("license build", result["build"], False)


def test_java_source_runs_full_build():
    """A Java change is relevant source and must take the build job."""
    result = build_relevant.decide(
        ["core/src/main/java/Foo.java"],
        source_changes=True,
    )
    check_equal("java relevant", result["build_relevant_changes"], True)
    check_equal("java compile-check", result["compile_check"], False)
    check_equal("java build", result["build"], True)


def test_empty_file_list_fails_closed():
    """A missing changed-file list must not skip the required build jobs."""
    result = build_relevant.decide([], source_changes=False)
    check_equal("empty relevant", result["build_relevant_changes"], True)
    check_equal("empty compile-check", result["compile_check"], True)
    check_equal("empty build", result["build"], False)


def test_normalize_keeps_dot_directories():
    """A leading ``./`` is stripped; ``.github`` must not lose its dot."""
    check_equal(
        "./docs/assets",
        build_relevant.normalize_path("./docs/assets/logo.png"),
        "docs/assets/logo.png",
    )
    check_equal(
        ".github",
        build_relevant.normalize_path(".github/workflows/build.yml"),
        ".github/workflows/build.yml",
    )
    result = build_relevant.decide(
        [".github/workflows/build.yml"],
        source_changes=True,
    )
    check_equal(".github still builds", result["build"], True)


def test_prefix_matching_does_not_over_ignore():
    """``web-v2-extra`` and ``docs/assets-backup`` are not the old ignore dirs."""
    check_equal(
        "web-v2-extra",
        build_relevant.is_ignored_path("web-v2-extra/package.json"),
        False,
    )
    check_equal(
        "docs/assets-backup",
        build_relevant.is_ignored_path("docs/assets-backup/logo.png"),
        False,
    )
    check_equal(
        "docs/open-api",
        build_relevant.is_ignored_path("docs/open-api/openapi.yaml"),
        False,
    )


def test_directory_roots_and_nested_paths_are_ignored():
    """Both the directory itself and nested files match the old globs."""
    for path in (
        "docs/assets",
        "docs/assets/nested/icon.svg",
        "web-v2",
        "web-v2/package.json",
    ):
        check_equal(path, build_relevant.is_ignored_path(path), True)


def test_workflow_yaml_stays_synced_with_python_model():
    """build.yml must call this module and gate both jobs on its output."""
    yaml_text = (REPO_ROOT / ".github" / "workflows" / "build.yml").read_text(
        encoding="utf-8"
    )
    check_equal(
        "sidecar entry point",
        "python3 dev/ci/build_relevant.py" in yaml_text,
        True,
    )
    check_equal(
        "compile-check gated",
        "needs.changes.outputs.build_relevant_changes == 'true'" in yaml_text
        and "needs.changes.outputs.source_changes != 'true'" in yaml_text,
        True,
    )
    check_equal(
        "build gated",
        "needs.changes.outputs.build_relevant_changes == 'true'" in yaml_text
        and "needs.changes.outputs.source_changes == 'true'" in yaml_text,
        True,
    )
    for glob in build_relevant.github_ignore_globs():
        quoted = f"'{glob}'"
        check_equal(f"push still ignores {glob}", quoted in yaml_text, True)
        check_equal(
            f"push-only glob {glob} is not the PR inner filter",
            yaml_text.count(quoted),
            1,
        )


def main():
    """Run build-relevant model tests without contacting GitHub."""
    tests = [
        test_docs_assets_only_is_a_noop,
        test_web_v2_src_skips_even_when_dorny_calls_it_source,
        test_mixed_web_v2_and_java_still_builds,
        test_non_source_relevant_file_runs_compile_check,
        test_java_source_runs_full_build,
        test_empty_file_list_fails_closed,
        test_normalize_keeps_dot_directories,
        test_prefix_matching_does_not_over_ignore,
        test_directory_roots_and_nested_paths_are_ignored,
        test_workflow_yaml_stays_synced_with_python_model,
    ]
    for test in tests:
        test()
        print(f"Passed {test.__name__}")
    print(f"Passed {len(tests)} build-relevant model tests")


if __name__ == "__main__":
    main()
