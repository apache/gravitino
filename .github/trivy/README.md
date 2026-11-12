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
# Vulnerability scan of shipped artifacts

The [CVE Scan](../workflows/cve-scan.yml) workflow builds the artifacts Gravitino ships and scans each
one with [Trivy](https://trivy.dev) for known HIGH and CRITICAL vulnerabilities. It runs every Monday
and can be started by hand from the Actions tab. It does not run on pull requests.

## What is scanned

Each artifact is a separate target, so a finding is attributed to the artifact users download.

| Job | Built with | Targets |
| --- | --- | --- |
| Server packages | `compileDistribution`, `compileIcebergRESTServer`, `compileLanceRESTServer` | `server-distribution`, `iceberg-rest-server`, `lance-rest-server` |
| Shaded jars | `shadowJar` of each module | the nine `*-bundle` jars, `client-java-runtime`, `filesystem-hadoop3-runtime`, `jobs`, `flink-runtime-{1.19,1.20,2.1,2.2}`, `spark-runtime-3.5_2.12`, `spark-runtime-3.5_2.13`, `spark-runtime-4.0` |
| Trino connectors | `copyRuntimeLibs` of each version range | `trino-connector-{469-472,473-477,478-479,480,481,482-483}` |
| Lock files | none | `mcp-server` (`mcp-server/uv.lock`), `web` (`web/web/pnpm-lock.yaml`), `web-v2` (`web-v2/web/pnpm-lock.yaml`), `client-python` (see below) |

Notes:

* The standalone Iceberg and Lance REST server directories contain the same third-party jars as
  their copies in `distribution/package`. They are scanned anyway because they are released on their own.
* `distribution/package-all` is not scanned: only `gravitino-<version>-bin.tar.gz` is released.
* The web UI is not built (`-PskipWeb=true`). Its WAR holds a minified static export and
  `WEB-INF/web.xml`, with no `package.json`, lock file or `node_modules`, so Trivy cannot inspect it.
  The dependencies compiled into the WARs are scanned through the two `pnpm-lock.yaml` files instead.
  Trivy 0.76.1 leaves out the development dependencies of pnpm lock files unless `--include-dev-deps` is
  set (`web`: 36 findings without the flag, 86 with it), so only what is shipped is gated.
* The Python client is scanned as pip installs it. `scan.sh stage-python clients/client-python <dir>/client-python`
  resolves the package with every extra except `dev` (read from the package, so new extras are covered)
  in a throwaway virtualenv and writes the result, including transitive dependencies, as `name==version`
  lines. This works whether the requirements files pin versions or allow ranges. The resolution is done
  for the project's `pythonVersion` (`gradle.properties`) on the Linux runner, and it can change when a
  dependency publishes a new release inside an allowed range; a new finding is then real for new
  installs. `requirements-ray-iceberg.txt` is not an extra and is not part of the package. The step
  fails when pip cannot resolve the package or no dependency is left, so an empty file cannot pass as a
  clean scan.
* The Docker images published by `dev/release` are not scanned yet. That is a follow-up.
* The Trino 473 and later modules need JDK 25, which Gradle provisions through its toolchain resolver.

## How the scan runs

* Trivy runs from a container image pinned by digest (see `scan.sh`), read-only, without capabilities
  and, for the scan itself, without network access.
* One job downloads the vulnerability DB and the Java DB once per run and caches them under a date key.
  Scan jobs restore the cache read-only and skip all DB updates, so every report of a run uses the same DB snapshot.
* A target fails when a HIGH or CRITICAL finding remains after the exemptions, or when Trivy did not
  produce a valid report. All targets are scanned even if one of them fails.
* One scan of a target is limited by `TRIVY_SCAN_TIMEOUT` (45m by default; a target that hits the limit
  fails). A scan takes seconds in practice, so the workflow sets a lower limit per job (15m for the
  server packages, 10m for Trino, 3m for jars and lock files). The limits of all targets of a job
  add up to less than its `Scan` step limit, so a hanging scan fails that step, and the reports of
  the other targets are still uploaded, instead of hitting the limit of the whole job.
* Each daily cache entry holds about 1 GB. It counts against the repository's 10 GB Actions cache, which
  is shared with the Gradle caches of every workflow (least recently used entries are evicted first), and
  a manual run on another branch stores its own entry. Start manual runs from the default branch and
  avoid repeating them.

## When a scheduled run fails

GitHub notifies only the user who last changed the `cron` line, so the workflow keeps a tracking issue
titled `Scheduled release artifact scan failed`. A failed scheduled run opens it, or adds a comment with
the run and the failed jobs when it is already open. The first scheduled run that passes again adds a
comment and closes it. Manual runs do not touch the issue. Committers triage the issue: fix the finding,
or update the exemptions following the rules below.

## Reading the results

* Each target has a section in the run summary: the DB snapshot, the remaining findings, and a
  collapsed table of the suppressed findings with the reason for each.
* The raw JSON, the SARIF and the Markdown report of every target are kept for 30 days as workflow
  artifacts (`cve-scan-*`).
* On `apache/gravitino` the SARIF reports are also sent to GitHub code scanning, when it is enabled.
* The `Exemptions` job fails when an entry in `trivyignore.yaml` or `vex.openvex.json` breaks the policy
  below (`scan.sh check-exemptions`). It also warns about entries that matched no finding in any target of
  the run (`scan.sh unused`). Remove those.

## Running the scan locally

You need Docker and `jq`. `yq` (v4) is only needed for `scan.sh unused` and `scan.sh check-exemptions`,
and `python3` with pip 22.2 or later plus network access for `scan.sh stage-python`. These tools are
preinstalled on the GitHub `ubuntu` runners.

```bash
# Build what you want to scan, for example the server packages and the AWS bundle.
./gradlew compileDistribution :bundles:aws-bundle:shadowJar -PskipWeb=true -x test

# Download the vulnerability DBs into ~/.cache/trivy (about 1.1 GB to download, 3.1 GB on disk).
.github/trivy/scan.sh download-db

# Scan a directory. Reports are written to ./trivy-reports.
.github/trivy/scan.sh scan server-distribution distribution/package

# A fat jar is scanned from a directory that holds only that jar.
.github/trivy/scan.sh stage /tmp/targets aws-bundle 'bundles/aws-bundle/build/libs/gravitino-aws-bundle-*.jar'
.github/trivy/scan.sh scan aws-bundle /tmp/targets/aws-bundle
```

Set `TRIVY_EXEMPTIONS=off` to see all findings without the exemptions, `TRIVY_CACHE_DIR` and
`TRIVY_REPORT_DIR` to change the directories. Run `.github/trivy/scan.sh` without arguments for the list of commands.

## Exemptions

Fix the vulnerability first: upgrade the dependency, or exclude it if it is not needed. Add an
exemption only when that is not possible yet. Exemptions are changed through a pull request.

Exemptions are global: they are applied to every target of the run and keyed by the versioned package
URL, so one entry covers every artifact that carries that exact version. Check all the places a
package version appears before pointing an entry at an issue that only fixes some of them.

There are two kinds:

| | A: not affected (`vex.openvex.json`) | B: not analysed yet (`trivyignore.yaml`) |
| --- | --- | --- |
| Meaning | Gravitino does not execute the vulnerable code | Pre-existing finding that has not been triaged |
| Evidence | Required: a `not_affected` statement with one of the OpenVEX justifications (usually `vulnerable_code_not_in_execute_path`) and an `impact_statement` that cites the code or analysis, valid for the artifact where it is reported | None, but a tracking issue is required |
| Scope | The affected package URL | One entry per vulnerability id, restricted to the exact package versions found |
| Expiry | None, reviewed like code | `expired_at`, at most 90 days ahead |

Rules for entries in `trivyignore.yaml`:

* Use the statement `Pre-existing finding, tracked in apache/gravitino#<issue>.` Point it at an issue that
  covers every artifact carrying the listed versions.
* List the package URLs with the version, for example `pkg:maven/com.google.guava/guava@32.1.3-jre`. A new version of the package is a new finding.
* When an entry expires its finding fails the scan again. Renew the date only after the finding has been
  looked at again, and only if the tracking issue is still open. Delete entries whose finding is fixed.

`scan.sh check-exemptions` fails the `Exemptions` job when an entry has no `id`, `purls`, `expired_at` or
`statement`, when a purl has no exact version, when `expired_at` is malformed, quoted, not in the future or
more than 90 days ahead (UTC), or when the statement does not reference `apache/gravitino#<number>`. It also
checks that the VEX file is OpenVEX JSON and that every statement is `not_affected`, with one of the five
OpenVEX justifications (`component_not_present`, `vulnerable_code_not_present`,
`vulnerable_code_not_in_execute_path`, `vulnerable_code_cannot_be_controlled_by_adversary`,
`inline_mitigations_already_exist`) and an `impact_statement`. Other statuses are rejected: Trivy also
suppresses a finding marked `fixed`, which would need no evidence, and `affected` or
`under_investigation` suppress nothing. A fixed dependency is shown by upgrading it, not by a statement.

The ignore file uses a Trivy feature that is marked experimental, as are `--vex` and `--show-suppressed`.
