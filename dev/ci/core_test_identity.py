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

"""Build and reconcile normalized core-test identity manifests.

Gradle writes one JUnit XML directory per Test task.  This tool turns those
reports into stable, backend-neutral identity multisets so the H2, MySQL, and
PostgreSQL lanes can prove that they exercised the same test contract.  It also
records status counts and elapsed test time for CI artifacts.
"""

import argparse
from collections import Counter
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET


SCHEMA_VERSION = 1
LANES = ("unit", "h2", "mysql", "postgresql")
DATABASE_LANES = LANES[1:]
MANIFEST_LANES = LANES
STATUS_KEYS = ("passed", "skipped", "failures", "errors")

BACKEND_NAME_PATTERN = r"h2|mysql|postgresql"
TEST_TEMPLATE_BACKEND_RE = re.compile(
    rf"\[(?P<backend>{BACKEND_NAME_PATTERN})\s+Backend\]", re.IGNORECASE
)
STATS_BACKEND_CLASS_RE = re.compile(
    rf"(?P<prefix>TestJdbcPartitionStatisticStorageIT)\$"
    rf"(?P<backend>{BACKEND_NAME_PATTERN})Test(?=$|\$)",
    re.IGNORECASE,
)
BACKEND_TOKEN_RE = re.compile(
    rf"(?<![A-Za-z0-9])(?P<backend>{BACKEND_NAME_PATTERN})(?![A-Za-z0-9])",
    re.IGNORECASE,
)
TRAILING_INVOCATION_INDEX_RE = re.compile(
    r"\[(?:#)?\d+\](?=(?:\s*\[BACKEND Backend\])?\s*$)", re.IGNORECASE
)


class ManifestError(ValueError):
    """Raised when test results cannot form a trustworthy manifest."""


def _local_name(tag):
    """Return an XML element name without its optional namespace."""
    return tag.rsplit("}", 1)[-1]


def _canonical_backend(value):
    """Return the canonical spelling of a recognized backend."""
    return value.lower()


def _classname_backend_markers(value):
    """Find backend markers in the backend-specific nested stats classes."""
    return {
        _canonical_backend(match.group("backend"))
        for match in STATS_BACKEND_CLASS_RE.finditer(value)
    }


def _test_name_backend_markers(value):
    """Find structured backend markers in a testcase name."""
    markers = {
        _canonical_backend(match.group("backend"))
        for match in TEST_TEMPLATE_BACKEND_RE.finditer(value)
    }
    markers.update(
        _canonical_backend(match.group("backend"))
        for match in BACKEND_TOKEN_RE.finditer(value)
    )
    return markers


def _normalize_classname(value):
    """Normalize backend-specific nested stats class names."""
    return STATS_BACKEND_CLASS_RE.sub(
        lambda match: f"{match.group('prefix')}$BackendTest", value
    )


def _normalize_test_name(value):
    """Normalize backend markers and trailing parameterized indices."""
    normalized = TEST_TEMPLATE_BACKEND_RE.sub("[BACKEND Backend]", value)
    normalized = BACKEND_TOKEN_RE.sub("BACKEND", normalized)
    return TRAILING_INVOCATION_INDEX_RE.sub("[INDEX]", normalized)


def normalize_identity(lane, classname, test_name):
    """Validate lane markers and return a normalized test identity pair."""
    classname = (classname or "").strip()
    test_name = (test_name or "").strip()
    if not classname or not test_name:
        raise ManifestError("Every <testcase> must have non-empty classname and name attributes")

    markers = _classname_backend_markers(classname) | _test_name_backend_markers(test_name)
    if lane == "unit" and markers:
        raise ManifestError(
            "Unit test result contains an explicit backend marker "
            f"{sorted(markers)}: {classname}::{test_name}"
        )
    if lane in DATABASE_LANES:
        foreign_markers = markers - {lane}
        if foreign_markers:
            raise ManifestError(
                f"{lane} test result contains foreign backend marker(s) "
                f"{sorted(foreign_markers)}: {classname}::{test_name}"
            )

    return (
        _normalize_classname(classname),
        _normalize_test_name(test_name),
    )


def _testcase_status(testcase):
    """Classify one JUnit testcase element."""
    child_tags = {_local_name(child.tag) for child in testcase}
    if "failure" in child_tags:
        return "failures"
    if "error" in child_tags:
        return "errors"
    if "skipped" in child_tags:
        return "skipped"
    return "passed"


def _testcase_duration(testcase, source_file):
    """Parse one JUnit testcase duration as a non-negative Decimal."""
    value = testcase.get("time", "0")
    try:
        duration = Decimal(value)
    except InvalidOperation as error:
        raise ManifestError(
            f"Invalid testcase duration {value!r} in {source_file}"
        ) from error
    if not duration.is_finite() or duration < 0:
        raise ManifestError(f"Invalid testcase duration {value!r} in {source_file}")
    return duration


def _identities_as_json(identities):
    """Convert an identity Counter to deterministic JSON records."""
    return [
        {"classname": classname, "name": name, "count": count}
        for (classname, name), count in sorted(identities.items())
    ]


def _identities_from_json(manifest, source_file):
    """Validate and restore an identity Counter from a manifest."""
    records = manifest.get("identities")
    if not isinstance(records, list):
        raise ManifestError(f"Manifest {source_file} has no identities list")

    identities = Counter()
    for record in records:
        if not isinstance(record, dict):
            raise ManifestError(f"Manifest {source_file} has an invalid identity record")
        classname = record.get("classname")
        name = record.get("name")
        count = record.get("count")
        if not isinstance(classname, str) or not classname:
            raise ManifestError(f"Manifest {source_file} has an invalid classname")
        if not isinstance(name, str) or not name:
            raise ManifestError(f"Manifest {source_file} has an invalid testcase name")
        if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
            raise ManifestError(f"Manifest {source_file} has an invalid identity count")
        identity = (classname, name)
        if identity in identities:
            raise ManifestError(f"Manifest {source_file} repeats identity {identity}")
        identities[identity] = count
    return identities


def _identity_digest(identities):
    """Return a stable digest of an identity multiset."""
    digest = hashlib.sha256()
    for (classname, name), count in sorted(identities.items()):
        digest.update(classname.encode("utf-8"))
        digest.update(b"\0")
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(count).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def build_manifest(lane, results_directory):
    """Parse Gradle JUnit XML reports and return a normalized manifest."""
    if lane not in MANIFEST_LANES:
        raise ManifestError(
            f"Unknown lane {lane!r}; expected one of {', '.join(MANIFEST_LANES)}"
        )

    results_directory = Path(results_directory)
    if not results_directory.is_dir():
        raise ManifestError(f"Results directory does not exist: {results_directory}")

    xml_files = sorted(results_directory.rglob("TEST-*.xml"))
    if not xml_files:
        raise ManifestError(f"No TEST-*.xml files found under {results_directory}")

    identities = Counter()
    statuses = Counter({key: 0 for key in STATUS_KEYS})
    duration = Decimal("0")
    source_files = []

    for xml_file in xml_files:
        relative_source = xml_file.relative_to(results_directory).as_posix()
        source_files.append(relative_source)
        try:
            root = ET.parse(xml_file).getroot()
        except (ET.ParseError, OSError) as error:
            raise ManifestError(f"Could not parse {xml_file}: {error}") from error

        testcases = (
            element
            for element in root.iter()
            if _local_name(element.tag) == "testcase"
        )
        for testcase in testcases:
            identity = normalize_identity(
                lane, testcase.get("classname"), testcase.get("name")
            )
            identities[identity] += 1
            statuses[_testcase_status(testcase)] += 1
            duration += _testcase_duration(testcase, relative_source)

    test_count = sum(identities.values())
    if test_count == 0:
        raise ManifestError(f"No <testcase> entries found under {results_directory}")
    if statuses["failures"] or statuses["errors"]:
        raise ManifestError(
            f"Lane {lane} contains {statuses['failures']} failure(s) and "
            f"{statuses['errors']} error(s)"
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "lane": lane,
        "successful": True,
        "test_count": test_count,
        "unique_identity_count": len(identities),
        "duration_seconds": float(duration),
        "status_counts": {key: statuses[key] for key in STATUS_KEYS},
        "source_files": source_files,
        "identity_digest": _identity_digest(identities),
        "identities": _identities_as_json(identities),
    }


def write_json(document, output_file):
    """Write one deterministic JSON document."""
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as output:
        json.dump(document, output, indent=2, sort_keys=True)
        output.write("\n")


def _load_manifest(manifest_file):
    """Load and validate the common fields of one manifest."""
    manifest_file = Path(manifest_file)
    try:
        with manifest_file.open(encoding="utf-8") as source:
            manifest = json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        raise ManifestError(f"Could not read manifest {manifest_file}: {error}") from error

    if not isinstance(manifest, dict):
        raise ManifestError(f"Manifest {manifest_file} must contain a JSON object")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ManifestError(f"Manifest {manifest_file} has an unsupported schema version")
    lane = manifest.get("lane")
    if lane not in MANIFEST_LANES:
        raise ManifestError(f"Manifest {manifest_file} has invalid lane {lane!r}")
    if manifest.get("successful") is not True:
        raise ManifestError(f"Manifest {manifest_file} is not successful")

    identities = _identities_from_json(manifest, manifest_file)
    test_count = manifest.get("test_count")
    if not isinstance(test_count, int) or isinstance(test_count, bool) or test_count <= 0:
        raise ManifestError(f"Manifest {manifest_file} has invalid test_count")
    if sum(identities.values()) != test_count:
        raise ManifestError(f"Manifest {manifest_file} identity counts do not match test_count")

    unique_identity_count = manifest.get("unique_identity_count")
    if (
        not isinstance(unique_identity_count, int)
        or isinstance(unique_identity_count, bool)
        or unique_identity_count <= 0
        or unique_identity_count != len(identities)
    ):
        raise ManifestError(
            f"Manifest {manifest_file} has invalid unique_identity_count"
        )

    statuses = manifest.get("status_counts")
    if not isinstance(statuses, dict):
        raise ManifestError(f"Manifest {manifest_file} has invalid status_counts")
    for key in STATUS_KEYS:
        value = statuses.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ManifestError(f"Manifest {manifest_file} has invalid status {key}")
    if sum(statuses[key] for key in STATUS_KEYS) != test_count:
        raise ManifestError(f"Manifest {manifest_file} statuses do not match test_count")
    if statuses["failures"] or statuses["errors"]:
        raise ManifestError(f"Manifest {manifest_file} contains failed tests")

    duration = manifest.get("duration_seconds")
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise ManifestError(f"Manifest {manifest_file} has invalid duration_seconds")

    source_files = manifest.get("source_files")
    if (
        not isinstance(source_files, list)
        or not source_files
        or any(not isinstance(source, str) or not source for source in source_files)
        or len(set(source_files)) != len(source_files)
    ):
        raise ManifestError(f"Manifest {manifest_file} has invalid source_files")

    identity_digest = manifest.get("identity_digest")
    if identity_digest != _identity_digest(identities):
        raise ManifestError(f"Manifest {manifest_file} has invalid identity_digest")

    return lane, manifest, identities


def _format_identity_difference(reference, actual):
    """Format a bounded explanation of a Counter mismatch."""
    differences = []
    for label, values in (("missing", reference - actual), ("extra", actual - reference)):
        for (classname, name), count in sorted(values.items())[:5]:
            differences.append(f"{label} {count} x {classname}::{name}")
    return "; ".join(differences)


def _format_identities(identities):
    """Format a bounded identity Counter for an error message."""
    return "; ".join(
        f"{count} x {classname}::{name}"
        for (classname, name), count in sorted(identities.items())[:5]
    )


def _load_split_manifests(manifest_files):
    """Load exactly one trustworthy manifest for each split lane."""
    manifest_files = [Path(path) for path in manifest_files]
    if len(manifest_files) != len(LANES):
        raise ManifestError(f"Expected exactly {len(LANES)} manifests, got {len(manifest_files)}")

    by_lane = {}
    counters = {}
    for manifest_file in manifest_files:
        lane, manifest, identities = _load_manifest(manifest_file)
        if lane not in LANES:
            raise ManifestError(
                f"Expected a split-lane manifest, got lane {lane} from {manifest_file}"
            )
        if lane in by_lane:
            raise ManifestError(f"Received more than one manifest for lane {lane}")
        by_lane[lane] = manifest
        counters[lane] = identities

    missing_lanes = set(LANES) - set(by_lane)
    if missing_lanes:
        raise ManifestError(f"Missing manifest lane(s): {', '.join(sorted(missing_lanes))}")
    return by_lane, counters


def _lane_summary(manifest, identities):
    """Return the evidence retained for one successfully loaded lane."""
    return {
        "test_count": manifest["test_count"],
        "unique_identity_count": manifest["unique_identity_count"],
        "duration_seconds": manifest["duration_seconds"],
        "status_counts": manifest["status_counts"],
        "source_file_count": len(manifest["source_files"]),
        "source_files": manifest["source_files"],
        "identity_digest": _identity_digest(identities),
    }


def reconcile_manifests(manifest_files):
    """Require four lanes and reconcile the three database identity multisets."""
    by_lane, counters = _load_split_manifests(manifest_files)

    reference = counters["h2"]
    for lane in DATABASE_LANES[1:]:
        if counters[lane] != reference:
            difference = _format_identity_difference(reference, counters[lane])
            raise ManifestError(
                f"Database identity mismatch between h2 and {lane}: {difference}"
            )

    unit_database_overlap = counters["unit"] & reference
    if unit_database_overlap:
        raise ManifestError(
            "Unit/database identity overlap: "
            f"{_format_identities(unit_database_overlap)}"
        )

    combined_statuses = {
        key: sum(by_lane[lane]["status_counts"][key] for lane in LANES)
        for key in STATUS_KEYS
    }
    lane_summaries = {
        lane: _lane_summary(by_lane[lane], counters[lane]) for lane in LANES
    }

    return {
        "schema_version": SCHEMA_VERSION,
        "successful": True,
        "database_identities_equal": True,
        "unit_database_disjoint": True,
        "database_test_count_per_lane": sum(reference.values()),
        "database_unique_identity_count": len(reference),
        "database_identity_digest": _identity_digest(reference),
        "combined_test_count": sum(by_lane[lane]["test_count"] for lane in LANES),
        "combined_duration_seconds": float(
            sum(
                (
                    Decimal(str(by_lane[lane]["duration_seconds"]))
                    for lane in LANES
                ),
                Decimal("0"),
            )
        ),
        "combined_status_counts": combined_statuses,
        "lanes": lane_summaries,
    }


def _create_argument_parser():
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest_parser = subparsers.add_parser(
        "manifest", help="Create one normalized manifest from Gradle JUnit XML"
    )
    manifest_parser.add_argument("--lane", required=True, choices=MANIFEST_LANES)
    manifest_parser.add_argument(
        "--results", required=True, type=Path, help="Directory containing TEST-*.xml"
    )
    manifest_parser.add_argument(
        "--output", required=True, type=Path, help="JSON manifest to write"
    )

    reconcile_parser = subparsers.add_parser(
        "reconcile", help="Reconcile unit and database lane manifests"
    )
    reconcile_parser.add_argument(
        "--manifests", required=True, nargs="+", type=Path, help="The four lane manifests"
    )
    reconcile_parser.add_argument(
        "--output", required=True, type=Path, help="Combined JSON summary to write"
    )
    return parser


def main(argv=None):
    """Run the command-line interface."""
    parser = _create_argument_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "manifest":
            document = build_manifest(args.lane, args.results)
        else:
            document = reconcile_manifests(args.manifests)
        write_json(document, args.output)
    except ManifestError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
