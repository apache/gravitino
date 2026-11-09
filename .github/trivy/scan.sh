#!/usr/bin/env bash
#
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
#
# Runs Trivy (in a hardened container) against built Gravitino artifacts. The same script is used
# by .github/workflows/cve-scan.yml and by contributors on their machines.
#
# Usage:
#   scan.sh pull                       Pull the pinned Trivy image (with retries).
#   scan.sh download-db                Download the vulnerability and Java DBs into the cache dir.
#   scan.sh db-info                    Print the snapshot dates of the cached DBs (job summary too).
#   scan.sh scan NAME PATH [TYPE]      Scan one target directory. TYPE is rootfs (default) or fs.
#   scan.sh scan-all DIR [TYPE]        Scan every sub-directory of DIR as its own target.
#   scan.sh stage DIR NAME GLOB        Copy the one jar matching GLOB (not *-empty.jar) to DIR/NAME.
#   scan.sh stage-requirements SRC DEST  Merge the pinned Python client requirements; fail if none.
#   scan.sh unused REPORT_DIR          Warn about exemptions that matched nothing in the reports.
#   scan.sh check-exemptions           Fail when an exemption violates the policy in README.md.
#
# A target fails when a HIGH or CRITICAL finding remains after the exemptions are applied, or when
# Trivy did not produce a valid report (fail closed). Reports are written to $TRIVY_REPORT_DIR as
# NAME.json (raw), NAME.sarif and NAME.md. A target also fails when its SARIF or Markdown report
# cannot be produced, in both 'scan' and 'scan-all'. Only 'unused' and 'check-exemptions' need yq (v4).
#
# Environment (all optional):
#   TRIVY_IMAGE         Pinned scanner image (defaults to the digest pinned below).
#   TRIVY_CACHE_DIR     Vulnerability DB cache directory (default: $HOME/.cache/trivy).
#   TRIVY_REPORT_DIR    Output directory (default: ./trivy-reports).
#   TRIVY_POLICY_DIR    Directory holding trivyignore.yaml and vex.openvex.json
#                       (default: the directory of this script).
#   TRIVY_EXEMPTIONS    Set to "off" to scan without exemptions (used to generate a baseline).
#   GITHUB_STEP_SUMMARY When set, the Markdown report is appended to it as well.

set -euo pipefail

DEFAULT_IMAGE='ghcr.io/aquasecurity/trivy:0.76.1@sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${TRIVY_IMAGE:-${DEFAULT_IMAGE}}"
CACHE_DIR="${TRIVY_CACHE_DIR:-${HOME}/.cache/trivy}"
REPORT_DIR="${TRIVY_REPORT_DIR:-$(pwd)/trivy-reports}"
POLICY_DIR="${TRIVY_POLICY_DIR:-${SCRIPT_DIR}}"
EXEMPTIONS="${TRIVY_EXEMPTIONS:-on}"
IGNORE_FILE="${POLICY_DIR}/trivyignore.yaml"
VEX_FILE="${POLICY_DIR}/vex.openvex.json"
MAX_EXEMPTION_DAYS=90
SCAN_TIMEOUT='45m'
MAX_ROWS=200

# Flags shared by every container: no network is added here, callers decide.
hardening=(--rm --read-only --cap-drop ALL --security-opt no-new-privileges
  --user "$(id -u):$(id -g)" --tmpfs /tmp)
trivy_env=(-e TRIVY_CACHE_DIR=/cache -e TRIVY_SKIP_VERSION_CHECK=true
  -e TRIVY_DISABLE_TELEMETRY=true -e TRIVY_NO_PROGRESS=true)

log() { printf '%s\n' "$*" >&2; }

abs_dir() { (cd "$1" && pwd); }

# Retries a command with exponential backoff: retry ATTEMPTS COMMAND...
retry() {
  local attempts=$1 delay=10 n=1
  shift
  until "$@"; do
    if ((n >= attempts)); then
      log "Command failed after ${attempts} attempts: $*"
      return 1
    fi
    log "Attempt ${n}/${attempts} failed, retrying in ${delay}s: $*"
    sleep "${delay}"
    delay=$((delay * 2))
    n=$((n + 1))
  done
}

cmd_pull() {
  retry 5 docker pull --quiet "${IMAGE}" >/dev/null
  log "Pulled ${IMAGE}"
}

download_db() {
  # The default repository chain (mirror.gcr.io, then ghcr.io) is kept on purpose.
  docker run "${hardening[@]}" -v "${CACHE_DIR}:/cache" "${trivy_env[@]}" "${IMAGE}" \
    image --download-db-only --no-progress &&
    docker run "${hardening[@]}" -v "${CACHE_DIR}:/cache" "${trivy_env[@]}" "${IMAGE}" \
      image --download-java-db-only --no-progress
}

cmd_download_db() {
  mkdir -p "${CACHE_DIR}"
  retry 4 download_db
  log "Vulnerability DB: $(db_snapshot)"
}

# Prints the update time of the trivy DB and the Java DB in the cache.
db_snapshot() {
  local vuln java
  vuln="$(jq -r '.UpdatedAt // "unknown"' "${CACHE_DIR}/db/metadata.json" 2>/dev/null || echo unknown)"
  java="$(jq -r '.UpdatedAt // "unknown"' "${CACHE_DIR}/java-db/metadata.json" 2>/dev/null || echo unknown)"
  printf 'trivy-db %s, java-db %s' "${vuln}" "${java}"
}

cmd_db_info() {
  local md
  md="$(mktemp)"
  printf 'Vulnerability DB snapshot: %s\n\n' "$(db_snapshot)" >"${md}"
  emit_summary "${md}"
  rm -f "${md}"
}

emit_summary() {
  cat "$1" || return 1
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat "$1" >>"${GITHUB_STEP_SUMMARY}" || return 1
  fi
}

# Writes the Markdown report for one target: write_report NAME JSON OUTPUT.
# Every step is checked explicitly: the function runs inside '||' lists, where errexit is off.
write_report() {
  local name=$1 json=$2 md=$3 remaining suppressed snapshot
  remaining="$(jq '[.Results[]?.Vulnerabilities[]?] | length' "${json}")" || return 1
  suppressed="$(jq '[.Results[]?.ExperimentalModifiedFindings[]?] | length' "${json}")" || return 1
  snapshot="$(db_snapshot)" || return 1
  {
    printf '### %s\n\n' "${name}" &&
      printf 'DB snapshot: %s. Remaining HIGH/CRITICAL: **%s**. Suppressed: %s.\n\n' \
        "${snapshot}" "${remaining}" "${suppressed}" || return 1
    if ((remaining > 0 || suppressed > 0)); then
      printf 'Tables are grouped by vulnerability and package and show at most %s rows. The JSON report has everything.\n\n' "${MAX_ROWS}" || return 1
    fi
    if ((remaining > 0)); then
      printf '#### Remaining findings\n\n' || return 1
      printf '| Vulnerability | Severity | Package | Installed | Fixed | Location |\n' || return 1
      printf '| --- | --- | --- | --- | --- | --- |\n' || return 1
      jq -r --argjson max "${MAX_ROWS}" '
        def esc: tostring | gsub("\\|"; "\\|");
        [.Results[]? | .Target as $t | .Vulnerabilities[]?
          | {id: .VulnerabilityID, sev: .Severity, pkg: .PkgName, ver: .InstalledVersion,
             fix: (.FixedVersion // "-"), url: (.PrimaryURL // ""), path: (.PkgPath // $t)}]
        | group_by([.id, .pkg, .ver])
        | map(.[0] + {paths: (map(.path) | unique)})
        | sort_by((if .sev == "CRITICAL" then 0 else 1 end), .id) | .[:$max][]
        | "| " + (if .url != "" then "[\(.id)](\(.url))" else .id end)
          + " | \(.sev) | \(.pkg | esc) | \(.ver | esc) | \(.fix | esc) | "
          + ((.paths[:3] | map("`" + esc + "`") | join("<br>"))
             + (if (.paths | length) > 3 then "<br>(+\((.paths | length) - 3) more)" else "" end)) + " |"
      ' "${json}" || return 1
      printf '\n' || return 1
    fi
    if ((suppressed > 0)); then
      printf '<details><summary>Suppressed findings (%s)</summary>\n\n' "${suppressed}" || return 1
      printf '| Vulnerability | Severity | Package | Installed | Status | Statement | Source | Locations |\n' || return 1
      printf '| --- | --- | --- | --- | --- | --- | --- | --- |\n' || return 1
      jq -r --argjson max "${MAX_ROWS}" '
        def esc: tostring | gsub("\\|"; "\\|");
        [.Results[]? | .Target as $t | .ExperimentalModifiedFindings[]?
          | {id: .Finding.VulnerabilityID, sev: .Finding.Severity, pkg: .Finding.PkgName,
             ver: .Finding.InstalledVersion, status: .Status,
             stmt: (.Statement // "-"), src: ((.Source // "-") | split("/") | last),
             path: (.Finding.PkgPath // $t)}]
        | group_by([.id, .pkg, .ver, .status, .stmt, .src])
        | map(.[0] + {n: length}) | sort_by(.pkg, .id) | .[:$max][]
        | "| \(.id) | \(.sev) | \(.pkg | esc) | \(.ver | esc) | \(.status) | \(.stmt | esc) | \(.src | esc) | \(.n) |"
      ' "${json}" || return 1
      printf '\n</details>\n\n' || return 1
    fi
  } >"${md}" || return 1
}

# Converts the JSON report to SARIF with one run per target and a unique id: make_sarif NAME SARIF.
# The result is only installed when it is valid JSON with at least one run.
make_sarif() {
  local name=$1 sarif=$2
  docker run "${hardening[@]}" --network none -v "$(abs_dir "${REPORT_DIR}"):/out" \
    "${IMAGE}" convert --format sarif --output "/out/${name}.sarif" "/out/${name}.json" || return 1
  jq --arg id "trivy/${name}/" \
    '.runs |= map(.automationDetails = {id: $id})' "${sarif}" >"${sarif}.tmp" || return 1
  jq -e '(.runs | type == "array") and (.runs | length >= 1)' "${sarif}.tmp" >/dev/null || return 1
  mv "${sarif}.tmp" "${sarif}" || return 1
}

# Scans one target: scan_target NAME PATH TYPE. Returns non-zero when the target fails the gate or
# any step fails. Callers use it in '||' lists, where errexit is off, so every step is checked.
scan_target() {
  local name=$1 path=$2 type=${3:-rootfs} json sarif md
  json="${REPORT_DIR}/${name}.json"
  sarif="${REPORT_DIR}/${name}.sarif"
  md="${REPORT_DIR}/${name}.md"

  if [[ ! -d "${path}" ]] || [[ -z "$(find "${path}" -mindepth 1 -print -quit)" ]]; then
    log "::error title=${name}::Scan target '${path}' does not exist or is empty"
    return 1
  fi
  if [[ ! -f "${CACHE_DIR}/db/trivy.db" || ! -f "${CACHE_DIR}/java-db/trivy-java.db" ]]; then
    log "::error title=${name}::Vulnerability DBs are missing in ${CACHE_DIR}; run 'scan.sh download-db'"
    return 1
  fi
  mkdir -p "${REPORT_DIR}" || return 1
  rm -f "${json}" "${sarif}" "${sarif}.tmp" "${md}" || return 1

  local policy_args=() mounts=()
  mounts=(-v "$(abs_dir "${path}"):/scan:ro" -v "${CACHE_DIR}:/cache:ro"
    -v "$(abs_dir "${REPORT_DIR}"):/out")
  if [[ "${EXEMPTIONS}" != "off" ]]; then
    mounts+=(-v "$(abs_dir "${POLICY_DIR}"):/policy:ro")
    policy_args+=(--ignorefile /policy/trivyignore.yaml --vex /policy/vex.openvex.json)
  fi

  log "::group::Scan ${name}"
  local rc=0
  # The scan itself never needs the network: DBs come from the read-only cache.
  docker run "${hardening[@]}" --network none "${mounts[@]}" "${trivy_env[@]}" "${IMAGE}" \
    "${type}" /scan --scanners vuln --severity HIGH,CRITICAL \
    --skip-db-update --skip-java-db-update --offline-scan --cache-backend memory \
    --show-suppressed --timeout "${SCAN_TIMEOUT}" ${policy_args[@]+"${policy_args[@]}"} \
    --format json --output "/out/${name}.json" || rc=$?
  log "::endgroup::"

  if ((rc != 0)) || ! jq -e '.SchemaVersion != null and (.ArtifactName | type == "string")' \
    "${json}" >/dev/null 2>&1; then
    log "::error title=${name}::Trivy did not produce a valid report (exit code ${rc})"
    return 1
  fi

  # SARIF contains the remaining findings only. A target whose SARIF or report cannot be produced
  # fails, and no partial file is left behind (the SARIF directory is uploaded as one unit).
  if ! make_sarif "${name}" "${sarif}"; then
    rm -f "${sarif}" "${sarif}.tmp"
    log "::error title=${name}::Could not produce a valid SARIF report"
    return 1
  fi
  if ! write_report "${name}" "${json}" "${md}" || ! emit_summary "${md}"; then
    rm -f "${md}"
    log "::error title=${name}::Could not write the Markdown report"
    return 1
  fi

  local remaining
  remaining="$(jq '[.Results[]?.Vulnerabilities[]?] | length' "${json}")" || return 1
  if [[ ! "${remaining}" =~ ^[0-9]+$ ]]; then
    log "::error title=${name}::Could not count the remaining findings"
    return 1
  fi
  if ((remaining > 0)); then
    log "::error title=${name}::${remaining} HIGH/CRITICAL findings remain after exemptions"
    return 1
  fi
}

# Copies exactly one jar matching GLOB into DEST/NAME so that it is scanned as its own target.
cmd_stage() {
  local dest=$1 name=$2 pattern=$3 file matches=()
  while IFS= read -r file; do
    [[ "${file}" == *-empty.jar ]] || matches+=("${file}")
  done < <(compgen -G "${pattern}" || true)
  if ((${#matches[@]} != 1)); then
    log "::error title=${name}::Expected exactly one jar matching '${pattern}', found ${#matches[@]}"
    return 1
  fi
  mkdir -p "${dest}/${name}"
  cp "${matches[0]}" "${dest}/${name}/"
}

# Merges the pinned requirements files of the Python client into DEST/requirements.txt:
# stage_requirements SRC_DIR DEST. Fails (fail closed) when the merged file has no pinned '==' line,
# for example because the glob matched nothing or the files only hold comments.
cmd_stage_requirements() {
  local src=$1 dest=$2 file out="$2/requirements.txt"
  mkdir -p "${dest}" || return 1
  for file in "${src}"/requirements*.txt; do
    [[ -f "${file}" ]] || continue
    case "${file##*/}" in
      requirements-dev.txt | requirements-ray-iceberg.txt) continue ;;
    esac
    grep -hv '^#' "${file}" || true
  done | sort -u >"${out}" || return 1
  if ! grep -Eq '^[^#[:space:]][^#]*==' "${out}"; then
    log "::error title=client-python::No pinned '==' requirement found in '${src}/requirements*.txt'"
    rm -rf "${dest}"
    return 1
  fi
}

# Fails when a target fails, and also when there is nothing to scan (fail closed).
cmd_scan_all() {
  local parent=$1 type=${2:-rootfs} dir rc=0 count=0
  for dir in "${parent}"/*/; do
    [[ -d "${dir}" ]] || continue
    count=$((count + 1))
    scan_target "$(basename "${dir}")" "${dir%/}" "${type}" || rc=1
  done
  if ((count == 0)); then
    log "::error title=Scan::No target directory found in '${parent}'"
    return 1
  fi
  return "${rc}"
}

require_yq() {
  command -v yq >/dev/null 2>&1 || { log 'yq (v4) is required for this command'; return 1; }
}

# Lists every exemption entry as "ID PURL" (PURL is empty for entries without purls).
list_exemptions() {
  # shellcheck disable=SC2016 # $id is a yq variable, not a shell one.
  yq -r '.vulnerabilities[]? | .id as $id
    | (.purls // [""])[] | $id + " " + .' "${IGNORE_FILE}"
}

cmd_unused() {
  local dir=$1 used entries count=0 reports
  require_yq || return 1
  reports="$(find "${dir}" -maxdepth 1 -name '*.json' | wc -l | tr -d ' ')"
  if ((reports == 0)); then
    log "No JSON reports found in ${dir}; skipping the unused exemption check"
    return 0
  fi
  used="$(find "${dir}" -maxdepth 1 -name '*.json' -exec jq -r '
      .Results[]?.ExperimentalModifiedFindings[]?
      | select(.Status == "ignored")
      | .Finding.VulnerabilityID + " " + (.Finding.PkgIdentifier.PURL // "")' {} + | sort -u)"
  entries="$(list_exemptions)"
  local summary=''
  while read -r id purl; do
    [[ -n "${id}" ]] || continue
    if [[ -n "${purl}" ]]; then
      grep -qxF "${id} ${purl}" <<<"${used}" && continue
    else
      grep -q "^${id} " <<<"${used}" && continue
    fi
    count=$((count + 1))
    log "::warning title=Unused exemption::${id} ${purl} matched nothing in ${reports} reports; remove it from trivyignore.yaml"
    summary+="| ${id} | ${purl:--} |"$'\n'
  done <<<"${entries}"

  local md="${dir}/unused-exemptions.md"
  {
    printf '### Unused exemptions\n\n'
    if ((count == 0)); then
      # shellcheck disable=SC2016 # The backticks are Markdown.
      printf 'Every exemption in `trivyignore.yaml` matched a finding (%s reports checked).\n\n' "${reports}"
    else
      printf '%s exemption(s) matched nothing in %s reports. Remove them if the finding is fixed.\n\n' \
        "${count}" "${reports}"
      printf '| Vulnerability | Package URL |\n| --- | --- |\n%s\n' "${summary}"
    fi
  } >"${md}"
  emit_summary "${md}"
}

# Prints one line per violation of the exemption policy in trivyignore.yaml (see README.md).
ignore_violations() {
  local now=$1
  yq -o=json '.vulnerabilities |= ((. // []) | map(. + {"__expired_at_tag": (.expired_at | tag)}))' \
    "${IGNORE_FILE}" | jq -r --argjson now "${now}" --argjson maxdays "${MAX_EXEMPTION_DAYS}" '
    def is_date: type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
      and ((try (. + "T00:00:00Z" | fromdateiso8601 | todate | .[0:10]) catch "") == .);
    def text: type == "string" and (gsub("\\s"; "") | length) > 0;
    if (.vulnerabilities | length) == 0 then "trivyignore.yaml has no entries" else empty end,
    (.vulnerabilities | to_entries[] | .key as $i | .value as $e
      | "entry \($i + 1)" + (if ($e.id | text) then " (\($e.id))" else "" end) as $label
      | ( if ($e.id | text) | not then "missing id" else empty end,
          if (($e.purls | type) != "array") or ($e.purls | length) == 0
            or ($e.purls | any(type != "string" or (test("^pkg:[^/]+/.+@[^@/]+$") | not)))
          then "purls must be a non-empty list of package URLs with an exact version" else empty end,
          if $e.expired_at == null then "missing expired_at"
          elif ($e.expired_at | is_date | not) then "malformed expired_at \($e.expired_at)"
          elif $e.__expired_at_tag != "!!timestamp" then "expired_at must be an unquoted date, as Trivy ignores a quoted one"
          else
            (($e.expired_at + "T00:00:00Z" | fromdateiso8601) - $now) as $left
            | if $left <= 0 then "expired_at \($e.expired_at) is not in the future"
              elif $left > $maxdays * 86400 then "expired_at \($e.expired_at) is more than \($maxdays) days ahead"
              else empty end
          end,
          if ($e.statement | text | not) then "missing statement"
          elif ($e.statement | contains("TRACKING_ISSUE")) then "statement still contains the TRACKING_ISSUE placeholder"
          elif ($e.statement | test("apache/gravitino#[0-9]+") | not)
          then "statement must reference a tracking issue as apache/gravitino#<number>"
          else empty end
        ) | $label + ": " + .)'
}

# Prints one line per violation of the OpenVEX rules for vex.openvex.json (see README.md).
vex_violations() {
  if ! jq -e . "${VEX_FILE}" >/dev/null 2>&1; then
    echo "not valid JSON"
    return 0
  fi
  jq -r '
    def text: type == "string" and (gsub("\\s"; "") | length) > 0;
    def justifications: ["component_not_present", "vulnerable_code_not_present",
      "vulnerable_code_not_in_execute_path", "vulnerable_code_cannot_be_controlled_by_adversary",
      "inline_mitigations_already_exist"];
    if type != "object" then "the document must be a JSON object" else
      ( if (.["@context"] | type) == "string" and (.["@context"] | startswith("https://openvex.dev/ns"))
        then empty else "@context must be an OpenVEX namespace (https://openvex.dev/ns/...)" end,
        if (.["@id"] | text) then empty else "missing @id" end,
        if (.author | text) then empty else "missing author" end,
        if (.timestamp | text) then empty else "missing timestamp" end,
        if (.version | type) == "number" and .version >= 1 then empty else "version must be a number >= 1" end,
        if (.statements | type) == "array" then empty else "statements must be a list" end,
        ( (.statements | if type == "array" then to_entries else [] end)[] | .key as $i | .value as $s
          | "statement \($i + 1)" + (if ($s.vulnerability.name | text) then " (\($s.vulnerability.name))" else "" end) as $label
          | ( if ($s.vulnerability.name | text) | not then "missing vulnerability.name" else empty end,
              if (($s.products | type) != "array") or ($s.products | length) == 0
                or ($s.products | any(((.["@id"] // "") | text) | not))
              then "products must be a non-empty list of products with an @id" else empty end,
              if ($s.status | text) | not then "missing status"
              elif (["not_affected", "fixed", "under_investigation", "affected"] | index($s.status)) == null
              then "unknown status \($s.status)"
              elif $s.status == "not_affected" then
                ( if (justifications | index($s.justification)) == null
                  then "not_affected needs a justification from the OpenVEX list" else empty end,
                  if ($s.impact_statement | text) | not
                  then "not_affected needs an impact_statement that cites the code or analysis" else empty end )
              elif $s.status == "affected" then
                ( if ($s.action_statement | text) | not then "affected needs an action_statement" else empty end )
              else empty end
            ) | $label + ": " + .)
      )
    end' "${VEX_FILE}"
}

cmd_check_exemptions() {
  require_yq || return 1
  local violations rc=0 count
  if [[ ! -f "${IGNORE_FILE}" || ! -f "${VEX_FILE}" ]]; then
    log "::error title=Exemption policy::trivyignore.yaml or vex.openvex.json is missing in ${POLICY_DIR}"
    return 1
  fi
  violations="$(
    ignore_violations "$(date -u +%s)" | sed 's/^/trivyignore.yaml: /' || echo 'trivyignore.yaml: cannot be parsed'
    vex_violations | sed 's/^/vex.openvex.json: /' || echo 'vex.openvex.json: cannot be checked'
  )"
  local md
  md="$(mktemp)"
  if [[ -z "${violations}" ]]; then
    printf '### Exemption policy\n\nAll exemptions follow the policy.\n\n' >"${md}"
  else
    count="$(grep -c '' <<<"${violations}")"
    rc=1
    log "::error title=Exemption policy::${count} violation(s), see the job summary and README.md"
    {
      printf '### Exemption policy\n\n%s violation(s). Showing at most %s.\n\n' "${count}" "${MAX_ROWS}"
      # shellcheck disable=SC2016 # The backticks are Markdown.
      printf '```\n%s\n```\n\n' "$(head -n "${MAX_ROWS}" <<<"${violations}")"
    } >"${md}"
  fi
  emit_summary "${md}" || rc=1
  rm -f "${md}"
  return "${rc}"
}

main() {
  local subcommand=${1:-}
  shift || true
  case "${subcommand}" in
    pull) cmd_pull ;;
    download-db) cmd_download_db ;;
    db-info) cmd_db_info ;;
    scan)
      [[ $# -ge 2 ]] || { log 'usage: scan.sh scan NAME PATH [rootfs|fs]'; exit 2; }
      scan_target "$@" || exit 1
      ;;
    scan-all)
      [[ $# -ge 1 ]] || { log 'usage: scan.sh scan-all DIR [rootfs|fs]'; exit 2; }
      cmd_scan_all "$@"
      ;;
    stage)
      [[ $# -eq 3 ]] || { log 'usage: scan.sh stage DIR NAME GLOB'; exit 2; }
      cmd_stage "$@"
      ;;
    stage-requirements)
      [[ $# -eq 2 ]] || { log 'usage: scan.sh stage-requirements SRC_DIR DEST_DIR'; exit 2; }
      cmd_stage_requirements "$@"
      ;;
    check-exemptions) cmd_check_exemptions ;;
    unused)
      [[ $# -ge 1 ]] || { log 'usage: scan.sh unused REPORT_DIR'; exit 2; }
      cmd_unused "$@"
      ;;
    *)
      log 'usage: scan.sh {pull|download-db|db-info|scan|scan-all|stage|stage-requirements|unused|check-exemptions} ...'
      exit 2
      ;;
  esac
}

main "$@"
