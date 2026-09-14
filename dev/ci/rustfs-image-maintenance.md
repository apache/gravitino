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

# Maintaining the RustFS test image

The shared fixture reads `services.rustfs.image` from
[`docker-compose-rustfs.yml`](../../integration-test-common/src/test/resources/docker-compose-rustfs.yml).
Keep this as the single authoritative `rustfs/rustfs:<release>@sha256:<manifest-digest>`
pin. Updating an unused Dockerfile or a second Java constant would not update the
fixture. The digest pins the multi-platform image index; CI scans its Linux amd64
image. Local arm64 availability does not establish identical vulnerability results.
See the [fixture guide](../../integration-test-common/README-rustfs.md) for runtime
compatibility checks and backend limitations.

## Image updates

[Dependabot](../../.github/dependabot.yml) checks that Compose directory weekly and
opens image-update PRs. GitHub's Docker updater
[preserves tag suffixes](https://github.com/dependabot/dependabot-core/blob/main/docker/README.md#supported-tag-schemas),
so do not assume that it will move `1.0.0-rc.6` to another release candidate or to
`1.0.0`. The **RustFS Image Maintenance** workflow also checks all published
[upstream releases](https://github.com/rustfs/rustfs/releases) weekly and on manual
dispatch. A newer numbered release fails its `releases` job with the current pin
and proposed release in the summary. Once on a stable version, this check ignores
prereleases. It reports availability; a maintainer must verify the corresponding
container tag and digest before updating the pin.

An image update must include release-note review, multi-platform manifest
verification and passing affected integration checks before merge. Existing
[`Backend Integration Test`](../../.github/workflows/backend-integration-test.yml)
path filters include `integration-test-common/**`. Its reusable workflow runs the
fileset and Iceberg REST server integration tests with Docker enabled in embedded
and deploy modes, including credential-vending authorization checks. Do not bypass
those tests or auto-merge an image update. Repository branch-protection settings
are separate from these workflow triggers.

## Vulnerability reports and their limits

The [maintenance workflow](../../.github/workflows/rustfs-image-maintenance.yml)
inventories the exact pin using Syft and scans that SBOM with Grype on pin changes,
on relevant workflow/helper changes, every Monday, and on manual dispatch. The
weekly schedule runs in `apache/gravitino`; forks can use manual dispatch.
Actions use commit hashes approved by the
[ASF allowlist](https://github.com/apache/infrastructure-actions/blob/main/actions.yml).
Their immutable action commits also select fixed scanner versions; the existing
GitHub Actions Dependabot entry proposes action updates.

Each run retains the image reference, Syft JSON SBOM, Grype JSON findings and a
summary for 30 days. The scan includes unfixed findings and all severities.
High/critical findings fail the job after reports are retained. Scanner, registry,
database and malformed-report failures are failures, not clean scans. Inspect the
raw report for package versions, vulnerability IDs, fixes and scanner/database
metadata. No Code Scanning upload or repository security setting is assumed.

The summary counts Rust packages and separately identifies packages discovered
in `cargo-auditable` binaries. A missing auditable inventory is an explicit
coverage gap, even if OS-package results are clean. Syft requires usable
[Rust dependency metadata](https://oss.anchore.com/docs/capabilities/rust/), such as
`Cargo.lock` or binaries built with `cargo-auditable`. An inventory generated from
an opaque binary cannot recover every statically linked Rust dependency. Even a
nonempty Rust inventory does not prove that every RustFS dependency was detected.
Also review [RustFS security advisories](https://github.com/rustfs/rustfs/security/advisories)
and its release notes. If upstream supplies an SBOM, verify that it describes the
exact image/platform before treating it as additional coverage.

The initial Linux amd64 scan on September 14, 2026 used the fixture's `1.0.0-rc.6`
image, Syft 1.51.1 and Grype 0.118.0 with that day's vulnerability database. It
inventoried 35 Alpine packages and zero Rust packages. Grype reported 16 medium
matches across five CVE IDs, with no high/critical matches. This baseline records
the observed Rust coverage gap; it is not a permanent vulnerability assessment.

## Maintainer response

Maintainers responsible for the test infrastructure should watch the
[workflow runs](https://github.com/apache/gravitino/actions/workflows/rustfs-image-maintenance.yml)
and configure GitHub Actions notifications for failures. A scheduled check only
becomes active after its workflow is on the default branch; confirm its first run
and the first Dependabot update check after merge. These files do not change
repository notification settings. The workflow has only `contents: read`, does
not require repository secrets, and creates no issues, comments or update commits.

For a failed run:

1. Download its reports. Distinguish an unavailable scanner/registry/database from
   a vulnerability or newer-release finding, and rerun transient failures.
2. For a public advisory, check applicability to the pinned image and test usage.
   Record the advisory IDs, affected package versions, fix availability and run
   link in one existing update PR or tracking issue; reuse that thread for later
   weekly findings. Use `dev@gravitino.apache.org` to coordinate an unowned update.
   Newly discovered or nonpublic vulnerabilities follow [SECURITY.md](../../SECURITY.md).
3. For an urgent applicable finding, open a focused image-update PR promptly and
   rerun the scan plus the affected integration suites. If no suitable fix exists,
   evaluate test isolation or a replacement backend with maintainers.
4. Any accepted exception needs an explicit rationale, advisory/image scope,
   responsible maintainer, expiry/revisit date and tracking link. Keep the finding
   visible; this workflow does not silently suppress accepted findings. Revisit
   the decision when the expiry date, image or upstream advisory changes.

The release job runs only on schedule/manual dispatch so a new upstream release
does not fail an unrelated PR. It intentionally remains actionable until the pin
is updated or maintainers document a decision to defer; the workflow does not
create repeated issue notifications.

## Local checks

Run the helper regression tests without Docker or third-party Python packages:

```bash
python3 -m unittest discover -s dev/ci -p test_rustfs_image.py -v
actionlint .github/workflows/rustfs-image-maintenance.yml
```

With Docker Compose, Syft and Grype installed, reproduce the image scan from the
repository root (the action commits record the scanner versions used in CI):

```bash
image=$(docker compose -f integration-test-common/src/test/resources/docker-compose-rustfs.yml config --format json | python3 dev/ci/rustfs_image.py image)
syft scan "registry:$image" --platform linux/amd64 -o syft-json=rustfs-sbom.json
grype sbom:rustfs-sbom.json -o json --file rustfs-vulnerabilities.json
python3 dev/ci/rustfs_image.py report --image "$image" --sbom rustfs-sbom.json --scan rustfs-vulnerabilities.json
gh api --paginate --slurp 'repos/rustfs/rustfs/releases?per_page=100' | jq 'add' > rustfs-releases.json
python3 dev/ci/rustfs_image.py releases --image "$image" --releases rustfs-releases.json
```
