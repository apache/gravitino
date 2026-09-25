<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Container image advisories

Keep release and test images maintained with ordinary package scans and reviewable
updates—not a zero-CVE gate or an exhaustive discovery system.

## Scope and cadence

The [maintained list](container-scan-images.json) includes published Gravitino
images, CI/compatibility fixtures, chart dependencies and test tooling. Each entry
records its purpose and source. Update it alongside release and fixture changes.
It is scan configuration, not the authoritative fixture pin; Dependabot does not
synchronize it. Preserve intentionally old compatibility versions where needed.

The [workflow](../../.github/workflows/container-image-advisories.yml) runs monthly
on the upstream default branch (first day, 07:23 UTC), manually, and after
successful publication through [Publish Docker Image](../../.github/workflows/docker-image.yml).
Manual input selects one image; blank input scans the list. Prefer a digest for
exact-artifact checks. The publication hook scans the published tag in a separate
read-only job without rebuilding or receiving publication secrets.

The hook applies only to revisions containing these workflows, not every external
release process. It resolves the tag at scan time and records its digest, not an
attestation that the tag still identifies the exact build just published.
Monthly cadence is a maintenance tradeoff, not a mandated security standard.
Run an extra scan for urgent advisories.

There are no PR/push scan triggers, merge gates, automatic tickets, SARIF uploads
or dependency-graph submissions. Findings and failures are warnings only.
Existing integration tests still validate actual image updates.

## Evidence and limits

Syft inventories each Linux amd64 registry image without running it. Grype matches
the saved package inventory against its vulnerability database. The report keeps
the resolved manifest digest, package types, all severities and unfixed findings.
Per-image SBOMs, raw vulnerability JSON and summaries are retained for 30 days.
Pinned actions use four-way parallelism, bounded timeouts and contents-read access.

This is ordinary metadata-based scanning, not exploit testing or bespoke binary
analysis. Missing metadata, opaque/static dependencies, other architectures,
runtime downloads and overrides are not fully covered. RustFS rc.6 has no
discovered Rust crates in the initial SBOM: OS-only findings do not establish
embedded Rust dependency coverage. Zero matches do not prove an image is safe.

We do not parse arbitrary Java, Gradle, shell or generated manifests to promise
whole-repository discovery. Reviewers must maintain the explicit list when images
change. Scan final published products, not only their bases. Unpublished snapshots
and local builds need a published reference or a separate build-specific check.

Failed downloads, empty inventories, missing platforms and invalid reports are
warning states, never clean results. Platform outages or cancellation can interrupt
a run. Check expected runs and artifacts; silence is not success. Scheduling starts
only after default-branch placement. Verify the first hosted scan after merge.

## Version updates and response

[Dependabot](../../.github/dependabot.yml) checks supported Docker/chart directories
and recursive Compose manifests weekly, with up to five open PRs per added
ecosystem and no auto-merge. Existing language/action updates are unchanged.
Java/Gradle image strings, computed Helm values and the scan list need manual
maintenance. Prerelease suffixes can limit updates; release-candidate-to-stable
transitions must not be assumed automatic. Verify the first hosted update check.

Version updates and vulnerability reporting are separate: Syft/Grype reports do
not create Dependabot alerts or GitHub issues.

Prioritize applicable findings in shipped products, especially exposed code,
known exploitation and available fixes. Then maintain CI/test fixtures while
preserving old-version compatibility coverage. A database-recorded package fix is
not proof of a compatible replacement image. Check vendor guidance, test and rescan
updates, and record a reason and revisit condition for deferrals. Avoid one issue
per match and blanket suppressions.

Use [the follow-up issue](https://github.com/apache/gravitino/issues/13157) for public
findings and focused updates. Nonpublic vulnerabilities follow
[SECURITY.md](../../SECURITY.md). The workflow does not configure notifications;
failure-only notifications miss warning-only findings.

## Local verification

```bash
python3 -m unittest discover -s dev/ci -p test_container_image_report.py -v
actionlint .github/workflows/container-image-advisories.yml
actionlint -shellcheck="" .github/workflows/docker-image.yml
```

To generate the same evidence for one image:

```bash
report_dir=$(mktemp -d)
image_ref=docker.io/apache/gravitino:1.3.0
syft scan "registry:$image_ref" --platform linux/amd64 -o "syft-json=$report_dir/sbom.json"
grype "sbom:$report_dir/sbom.json" -o json --file "$report_dir/vulnerabilities.json"
python3 dev/ci/container_image_report.py --image "$image_ref" --directory "$report_dir"
```

If a scanner fails, pass the actual `--sbom-outcome` and `--scan-outcome`
to the reporter. Missing evidence must never be treated as clean.

## Rationale

[NIST SSDF](https://csrc.nist.gov/projects/ssdf) supports risk-based practices;
[NIST SP 800-190](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-190.pdf)
recommends image vulnerability management. Neither mandates this monthly cadence.
[Grype architecture](https://oss.anchore.com/docs/architecture/grype/) and
[result interpretation](https://oss.anchore.com/docs/guides/vulnerability/interpreting-results/)
explain package matching and its limits.
