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

Existing dependency monitoring did not cover container images. The
[Container Image Advisories workflow](../../.github/workflows/container-image-advisories.yml)
adds repository-wide discovery and advisory vulnerability reports. It runs weekly
on the upstream default branch and on manual dispatch, with no PR or push check.
Vulnerabilities, missing coverage, unavailable images and scanner failures produce
warnings only. None is a merge or build gate.

## What is inventoried

The collector reads tracked source files; it does not execute Gradle, Java, shell
commands or Helm templates. Each run retains a deduplicated inventory with the
declared reference, source file/line, normalized registry reference and unresolved
declarations. Docker Hub shorthand aliases are deduplicated and untagged images
are recorded as `latest`. These are scan targets, not rewritten source pins.

| Declaration | Discovery coverage | Version-update coverage |
| --- | --- | --- |
| Dockerfiles/Containerfiles | Literal `FROM` and external `COPY --from` references, with global `ARG` defaults; excludes stage aliases and `scratch` | Dependabot Docker directories for `FROM`; external copy sources may need manual updates |
| Compose files | Service `image` values, including nested literal environment defaults; local `build` services recorded separately | Dependabot Compose; its handling of interpolation and prerelease suffixes is limited |
| GitHub Actions / Kubernetes | Literal service images, job containers and `docker://` actions | No additional automatic update coverage claimed for CI services/container actions |
| Java/Testcontainers and Gradle | Image constants, literal constructor/`withImage`/`DockerImageName.parse` arguments, image enums and declared CI image environment defaults | Manual source updates; Gradle Dependabot does not update these literals |
| Helm | Explicit repository/registry/tag/digest defaults and literal template image lines | Dependabot Docker scans chart default directories; computed tags/templates may need manual updates |
| Shell tooling | Literal `docker run`/`pull` targets with recognized options | Manual source updates |

The initial upstream inventory contained 41 unique registry references across
nine Dockerfiles, five Compose files, the CI MySQL service, shared fixtures,
maintenance tests, Gradle CI defaults, three Helm charts and two Docker helper
scripts. Families include the Gravitino/CI/playground images; Ubuntu, Debian,
Temurin and Python bases; MySQL, PostgreSQL, Doris, ClickHouse, OceanBase and
StarRocks; Kafka, ZooKeeper, Hive/Trino/Ranger; MinIO, Moto, LocalStack, uv and curl.
The generated artifact is authoritative for each run, not this historical count.
RustFS is automatically included when its Compose manifest is present; this
workflow does not depend on that migration.

The initial inventory also recorded 17 unresolved declarations: runtime image
overrides, computed Helm templates and the local Ranger build image. Defaults
are inventoried independently from those overrides. A caller can select other
images at runtime, and a locally published snapshot is not necessarily the current
registry snapshot. Dynamically concatenated refs, image references supplied only
through private configuration, implicit Testcontainers helper images such as
Ryuk, downloaded/generated Dockerfiles and local build outputs are not covered.
Transitive chart defaults are not expanded. Documentation examples, Helm assertion
fixtures, symlinks and the scanner's own
matrix expression are excluded. The collector is deliberately not a complete
Java/Kotlin/shell interpreter; new reference forms need discovery tests.

## Version updates

[Dependabot](../../.github/dependabot.yml) checks Dockerfile/chart directories and
recursively discovers Compose manifests weekly. It proposes ordinary reviewable
PRs and does not auto-merge. The recursive Compose configuration also discovers
new fixture manifests without a duplicate unused image list.

GitHub's Docker updater [preserves tag suffixes](https://github.com/dependabot/dependabot-core/blob/main/docker/README.md#supported-tag-schemas).
Do not assume that it proposes every release-candidate update or the transition
to a stable tag. Review upstream releases/advisories for image families whose
versions need manual updates. Version availability never fails this monitoring
workflow. Existing tests still validate any actual image update through their
normal changed-path triggers; this workflow adds no required check.

## Scan evidence and limitations

Syft inventories each registry reference's Linux amd64 image without running it.
Grype scans that retained SBOM, so a mutable tag cannot change between inventory
and vulnerability matching. The per-image summary records the resolved manifest
digest; source references that already include a digest retain it. The immutable
action hashes are on the [ASF allowlist](https://github.com/apache/infrastructure-actions/blob/main/actions.yml)
and select fixed scanner versions. GitHub Actions Dependabot can propose action
updates. Scanner and registry access use no repository secrets.

Inventory, SBOM, raw findings and summaries are retained for 30 days. All severities
and unfixed findings remain visible. Failed downloads, timeouts, invalid reports,
empty inventories and missing platform images are warning states, never evidence
of a clean scan. Individual jobs continue independently, with four scans running
at a time. Job/step failures are nonblocking; runner cancellation or a platform
outage can still interrupt a run. Check that an expected weekly run produced its
inventory and per-image artifacts rather than treating silence as success.
If discovery exceeds GitHub's 256-job matrix limit, it retains the full inventory
and warns that scans must be split; it does not silently scan only part of it.

Scanners depend on available package metadata and advisory databases. Opaque,
statically linked dependencies, unsupported package types, local build changes,
runtime downloads and other architectures may be absent. For example, the
initial RustFS rc.6 amd64 check discovered Alpine packages but zero Rust packages.
Usable `Cargo.lock`, [cargo-auditable metadata](https://oss.anchore.com/docs/capabilities/rust/)
or a verified upstream SBOM is needed to establish embedded Rust coverage. A
nonempty language inventory still does not prove that every dependency was found.

## Maintainer response and optional tickets

Review [workflow summaries and artifacts](https://github.com/apache/gravitino/actions/workflows/container-image-advisories.yml)
and upstream security advisories. Configure notifications or periodically inspect
the workflow; these files do not change repository notification settings. Since
findings are warnings, failure-only notifications are insufficient. Confirm the
first scheduled run and Dependabot check after merge.

Triage public findings in an existing update PR or tracking issue: record the
image reference and resolved digest, package/advisory IDs, applicability, available
fixes and run link. Reuse the thread for repeat findings. For an urgent applicable
finding, coordinate a focused update and its normal tests with maintainers via
`dev@gravitino.apache.org`. If deferring, record rationale, owner and revisit date.
New or nonpublic vulnerabilities follow [SECURITY.md](../../SECURITY.md).

Automatic ticket filing is possible, but is **not enabled or implemented here**.
A future explicit opt-in could maintain one rolling issue per normalized image
repository, grouping its affected declared tags/digests. A stable hidden marker
would locate the existing issue, and a digest/advisory fingerprint would suppress
unchanged updates. Only trusted default-branch scheduled runs would write issues;
new or changed findings would update that issue, and a successful complete scan
could record resolution. Failed/incomplete scans must not close findings.
This would require separately enabling `issues: write` and choosing triage owners
and notification policy. The current workflow has only `contents: read`, no
ticket-filing switch, and makes no issue, comment or repository-setting writes.

## Local verification

```bash
python3 -m venv /tmp/container-image-venv
/tmp/container-image-venv/bin/pip install -r dev/ci/requirements-container-images.txt
/tmp/container-image-venv/bin/python -m unittest discover -s dev/ci -p test_container_images.py -v
/tmp/container-image-venv/bin/python dev/ci/container_images.py inventory --root . --output /tmp/container-inventory
actionlint .github/workflows/container-image-advisories.yml
```

To scan one discovered reference with the scanner versions selected by the action
commits, then produce the same advisory summary:

```bash
mkdir -p /tmp/container-report
image=docker.io/library/ubuntu:22.04
syft scan "registry:$image" --platform linux/amd64 -o syft-json=/tmp/container-report/sbom.json
grype sbom:/tmp/container-report/sbom.json -o json --file /tmp/container-report/vulnerabilities.json
python3 dev/ci/container_images.py report --image "$image" --directory /tmp/container-report
```
