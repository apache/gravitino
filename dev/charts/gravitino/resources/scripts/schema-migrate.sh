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

# Entity-store schema init for the chart schema-migrate Job.
# Fail on any unexpected SQL error so a broken migration cannot roll out.
set -euo pipefail

: "${DB_ENGINE:?DB_ENGINE must be mysql or postgresql}"
: "${DB_HOST:?DB_HOST is required}"
: "${GRAVITINO_DB:?GRAVITINO_DB is required}"
: "${SCRIPTS_DIR:=/scripts}"

if [ "$DB_ENGINE" != mysql ] && [ "$DB_ENGINE" != postgresql ]; then
  echo "ERROR: DB_ENGINE must be mysql or postgresql (got ${DB_ENGINE})" >&2
  exit 1
fi

if [ "$DB_ENGINE" = mysql ]; then
  : "${MYSQL_USER:?MYSQL_USER is required}"
  : "${MYSQL_PWD:?MYSQL_PWD is required}"
  export MYSQL_PWD
else
  : "${POSTGRES_USER:?POSTGRES_USER is required}"
  : "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
fi

DB_PORT="${DB_PORT:-}"
PG_CURRENT_SCHEMA="${PG_CURRENT_SCHEMA:-}"
BASELINE_VERSION="${BASELINE_VERSION:-}"
VERSION_FILE="${SCRIPTS_DIR}/version.txt"

mysql_client() {
  local extra=()
  if [ -n "$DB_PORT" ]; then
    extra+=(-P "$DB_PORT")
  fi
  mysql -h "$DB_HOST" "${extra[@]}" -u "$MYSQL_USER" "$@"
}

psql_client() {
  local extra=()
  if [ -n "$DB_PORT" ]; then
    extra+=(-p "$DB_PORT")
  fi
  PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" "${extra[@]}" -U "$POSTGRES_USER" "$@"
}

version_le() {
  [ "$(printf '%s\n%s' "$1" "$2" | sort -V | head -n1)" = "$1" ]
}

version_lt() {
  [ "$1" != "$2" ] && version_le "$1" "$2"
}

require_semver() {
  local name="$1"
  local ver="$2"
  if [[ ! "${ver}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: ${name} must be X.Y.Z (got '${ver}')" >&2
    exit 1
  fi
}

wait_for_db() {
  echo "Waiting for ${DB_ENGINE} at ${DB_HOST}..."
  if [ "$DB_ENGINE" = mysql ]; then
    until mysql_client -e "SELECT 1" >/dev/null 2>&1; do
      echo "Waiting for MySQL to be ready..."
      sleep 2
    done
    # Server ready does not mean Bitnami (or the operator) has created GRAVITINO_DB yet.
    until mysql_client -D "$GRAVITINO_DB" -e "SELECT 1" >/dev/null 2>&1; do
      echo "Waiting for MySQL database ${GRAVITINO_DB}..."
      sleep 2
    done
  else
    until psql_client -d "$GRAVITINO_DB" -c "SELECT 1" >/dev/null 2>&1; do
      echo "Waiting for PostgreSQL to be ready..."
      sleep 2
    done
  fi
}

mysql_query() {
  mysql_client -D "$GRAVITINO_DB" -sNe "$1"
}

mysql_exec() {
  mysql_client -D "$GRAVITINO_DB" -e "$1"
}

psql_query() {
  psql_client -d "$GRAVITINO_DB" -v ON_ERROR_STOP=1 -tAc "$1"
}

db_query() {
  if [ "$DB_ENGINE" = mysql ]; then
    mysql_query "$1"
  else
    psql_query "$1"
  fi
}

# Community upgrade-*-mysql.sql uses ADD COLUMN / DROP INDEX without IF NOT EXISTS.
# Ignore "already applied" errors so re-runs and overlapping snapshots stay safe.
mysql_is_already_applied_error() {
  case "$1" in
    1060 | 1061 | 1068 | 1091 | 1176 | 1280) return 0 ;;
    *) return 1 ;;
  esac
}

mysql_apply_sql_file() {
  local file="$1"
  local out unexpected code line rc
  set +e
  out="$(mysql_client -D "$GRAVITINO_DB" --force <"$file" 2>&1)"
  rc=$?
  set -e
  if [ -n "${out}" ]; then
    printf '%s\n' "${out}"
  fi
  unexpected=""
  while IFS= read -r line; do
    [ -z "${line}" ] && continue
    code="$(printf '%s' "${line}" | sed -n 's/.*ERROR \([0-9][0-9]*\).*/\1/p')"
    [ -n "${code}" ] || continue
    if mysql_is_already_applied_error "${code}"; then
      echo "INFO: ignoring already-applied MySQL error ${code}"
      continue
    fi
    unexpected="${unexpected}${line}"$'\n'
  done < <(printf '%s\n' "${out}" | grep -E 'ERROR [0-9]+' || true)
  if [ -n "${unexpected}" ]; then
    echo "ERROR: SQL script failed: $file" >&2
    printf '%s' "${unexpected}" >&2
    exit 1
  fi
  if [ "${rc}" != 0 ] && ! printf '%s\n' "${out}" | grep -qE 'ERROR [0-9]+'; then
    echo "ERROR: mysql client failed: $file" >&2
    printf '%s\n' "${out}" >&2
    exit 1
  fi
}

apply_sql_file() {
  local file="$1"
  echo "Applying $file"
  if [ "$DB_ENGINE" = mysql ]; then
    mysql_apply_sql_file "$file"
  else
    if ! psql_client -d "$GRAVITINO_DB" -v ON_ERROR_STOP=1 -f "$file"; then
      echo "ERROR: SQL script failed: $file" >&2
      exit 1
    fi
  fi
}

# 1.3.2 widened table_column_version_info.column_name. Existing 1.3.x DBs may
# already record a later version without that ALTER; repair before ready.
ensure_column_name_capacity() {
  version_le 1.3.2 "$VERSION" || return 0

  local scope capacity
  if [ "$DB_ENGINE" = mysql ]; then
    scope='DATABASE()'
  else
    scope='current_schema()'
  fi
  capacity=$(db_query "SELECT character_maximum_length FROM information_schema.columns
    WHERE table_schema = ${scope} AND table_name = 'table_column_version_info'
      AND column_name = 'column_name';" | tr -d '[:space:]')
  case "$capacity" in
    '' | *[!0-9]*)
      echo "ERROR: Cannot determine table_column_version_info.column_name capacity" >&2
      exit 1
      ;;
  esac
  [ "$capacity" -lt 300 ] || return 0

  echo "Expanding table_column_version_info.column_name from ${capacity} to 300 characters"
  if [ "$DB_ENGINE" = mysql ]; then
    mysql_exec "ALTER TABLE table_column_version_info
      MODIFY COLUMN column_name VARCHAR(300) NOT NULL COMMENT 'column name';"
  else
    psql_query "ALTER TABLE table_column_version_info
      ALTER COLUMN column_name TYPE VARCHAR(300);" >/dev/null
  fi
}

ensure_migration_table() {
  if [ "$DB_ENGINE" = mysql ]; then
    mysql_exec "CREATE TABLE IF NOT EXISTS gravitino_schema_migration (
      id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
      schema_version VARCHAR(64) NOT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
  else
    psql_query "CREATE TABLE IF NOT EXISTS gravitino_schema_migration (
      id SMALLINT NOT NULL PRIMARY KEY,
      schema_version VARCHAR(64) NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );" >/dev/null
  fi
}

read_recorded_version() {
  db_query "SELECT schema_version FROM gravitino_schema_migration WHERE id = 1;" 2>/dev/null | tr -d '[:space:]' || true
}

write_recorded_version() {
  local ver="$1"
  if [ "$DB_ENGINE" = mysql ]; then
    mysql_exec "INSERT INTO gravitino_schema_migration (id, schema_version) VALUES (1, '${ver}')
      ON DUPLICATE KEY UPDATE schema_version = '${ver}';"
  else
    psql_query "INSERT INTO gravitino_schema_migration (id, schema_version, updated_at)
      VALUES (1, '${ver}', NOW())
      ON CONFLICT (id) DO UPDATE SET schema_version = EXCLUDED.schema_version, updated_at = NOW();" >/dev/null
  fi
  echo "Recorded schema version ${ver}"
}

metalake_exists() {
  if [ "$DB_ENGINE" = mysql ]; then
    local count
    count=$(mysql_query "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${GRAVITINO_DB}' AND table_name='metalake_meta';" 2>/dev/null || echo 0)
    [ "$count" = "1" ]
  else
    local exists
    # Limit to current_schema() so a populated public catalog cannot skip
    # initialization of an empty operator-selected schema (PGOPTIONS search_path).
    exists=$(psql_query \
      "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'metalake_meta');" 2>/dev/null || echo f)
    [ "$exists" = "t" ]
  fi
}

extract_table_names() {
  # The backticks are literal MySQL identifier quotes, not command substitution.
  # shellcheck disable=SC2016
  grep -io 'create table if not exists [`"]*[a-z0-9_]*[`"]*\|create table [`"]*[a-z0-9_]*[`"]*' "$1" |
    sed 's/.*[Tt][Aa][Bb][Ll][Ee] //; s/[Ii][Ff] [Nn][Oo][Tt] [Ee][Xx][Ii][Ss][Tt][Ss] //; s/[`"]//g'
}

schema_tables_exist() {
  local schema_sql="$1"
  local tbl exists
  local found=0
  while IFS= read -r tbl; do
    [ -z "$tbl" ] && continue
    found=1
    if [ "$DB_ENGINE" = mysql ]; then
      exists=$(mysql_query "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${GRAVITINO_DB}' AND table_name='${tbl}';")
      [ "$exists" = "1" ] || return 1
    else
      exists=$(psql_query "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '${tbl}');")
      [ "$exists" = "t" ] || return 1
    fi
  done < <(extract_table_names "$schema_sql")
  [ "$found" = "1" ]
}

column_exists() {
  local table="$1"
  local column="$2"
  local count
  if [ "$DB_ENGINE" = mysql ]; then
    count=$(mysql_query "SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema='${GRAVITINO_DB}' AND table_name='${table}' AND column_name='${column}'")
    [ "$count" = "1" ]
  else
    count=$(psql_query "SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema=current_schema() AND table_name='${table}' AND column_name='${column}'" | tr -d '[:space:]')
    [ "$count" = "1" ]
  fi
}

# Emit table<TAB>column for each ADD COLUMN in an upgrade script.
# Tracks the latest ALTER TABLE target so multi-line MySQL upgrades work.
extract_upgrade_add_columns() {
  local file="$1"
  local table="" line col
  while IFS= read -r line || [ -n "$line" ]; do
    if printf '%s\n' "$line" | grep -qiE 'alter[[:space:]]+table'; then
      table=$(printf '%s\n' "$line" | sed -E \
        's/.*[Aa][Ll][Tt][Ee][Rr][[:space:]]+[Tt][Aa][Bb][Ll][Ee][[:space:]]+([Ii][Ff][[:space:]]+[Ee][Xx][Ii][Ss][Tt][Ss][[:space:]]+)?[`"]?([A-Za-z0-9_]+).*/\2/')
      if [ "$table" = "$line" ]; then
        table=""
      fi
    fi
    if [ -n "$table" ] && printf '%s\n' "$line" | grep -qiE 'add[[:space:]]+column'; then
      col=$(printf '%s\n' "$line" | sed -E \
        's/.*[Aa][Dd][Dd][[:space:]]+[Cc][Oo][Ll][Uu][Mm][Nn][[:space:]]+([Ii][Ff][[:space:]]+[Nn][Oo][Tt][[:space:]]+[Ee][Xx][Ii][Ss][Tt][Ss][[:space:]]+)?[`"]?([A-Za-z0-9_]+).*/\2/')
      if [ -n "$col" ] && [ "$col" != "$line" ]; then
        printf '%s\t%s\n' "$table" "$col"
      fi
    fi
  done <"$file"
}

# Snapshots such as 1.3.0 and 2.0.0 share the same CREATE TABLE set; require
# columns introduced by upgrade-*-to-<ver> before claiming that version.
schema_version_markers_present() {
  local ver="$1"
  local f table column
  local found_marker=0
  shopt -s nullglob
  local files=("${SCRIPT_DB_DIR}"/upgrade-*-to-"${ver}"-"${DB_ENGINE}".sql)
  shopt -u nullglob
  if [ ${#files[@]} -eq 0 ]; then
    return 0
  fi
  for f in "${files[@]}"; do
    while IFS=$'\t' read -r table column; do
      if [ -z "$table" ] || [ -z "$column" ]; then
        continue
      fi
      found_marker=1
      column_exists "$table" "$column" || return 1
    done < <(extract_upgrade_add_columns "$f")
  done
  # Upgrade scripts exist but none ADD COLUMN (index-only / constraint-only).
  # Vacuous success would claim this snapshot for an older DB that shares the
  # same CREATE TABLE set and skip those upgrades. Treat markers as absent so
  # inference keeps the prior version (or requires schema.baselineVersion).
  [ "$found_marker" = "1" ]
}

infer_schema_version() {
  local best="" f ver
  local matched_without_markers=""
  shopt -s nullglob
  local files=("${SCRIPT_DB_DIR}"/schema-*-"${DB_ENGINE}".sql)
  shopt -u nullglob
  if [ ${#files[@]} -eq 0 ]; then
    return 0
  fi
  while IFS= read -r f; do
    ver=$(basename "$f" | sed "s/schema-\\(.*\\)-${DB_ENGINE}\\.sql/\\1/")
    if version_le "$ver" "$VERSION" && schema_tables_exist "$f"; then
      if schema_version_markers_present "$ver"; then
        best="$ver"
        matched_without_markers=""
      else
        # Tables match a newer snapshot whose upgrade columns are missing
        # (e.g. 1.3.0 DB vs schema-2.0.0.sql). Keep the prior best.
        matched_without_markers="$ver"
      fi
    fi
  done < <(printf '%s\n' "${files[@]}" | sort -V)
  if [ -n "$matched_without_markers" ] && [ -z "$best" ]; then
    echo "ERROR: Existing tables match schema-${matched_without_markers} but distinguishable upgrade markers (ADD COLUMN) for that version are missing." >&2
    echo "Cannot safely infer the starting schema version." >&2
    echo "Set schema.baselineVersion to the Gravitino version that created this database." >&2
    exit 1
  fi
  if [ -n "$matched_without_markers" ] && [ -n "$best" ] && version_lt "$best" "$matched_without_markers"; then
    # Keep diagnostics on stderr: callers capture stdout as the version only
    # (gravitino_schema_migration.schema_version is VARCHAR(64)).
    echo "INFO: Tables also match schema-${matched_without_markers}, but upgrade ADD COLUMN markers are absent; inferring ${best}." >&2
  fi
  printf '%s' "$best"
}

select_schema_file() {
  local best="" f ver
  shopt -s nullglob
  local files=("${SCRIPT_DB_DIR}"/schema-*-"${DB_ENGINE}".sql)
  shopt -u nullglob
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    ver=$(basename "$f" | sed "s/schema-\\(.*\\)-${DB_ENGINE}\\.sql/\\1/")
    if version_le "$ver" "$VERSION"; then
      best="$f"
    fi
  done < <(printf '%s\n' "${files[@]}" | sort -V)
  printf '%s' "$best"
}

# Patch releases (e.g. 1.3.1) ship no upgrade-* SQL; chain from the schema
# baseline X.Y.0 so upgrade-1.3.0-to-2.0.0 still runs when the DB records 1.3.1.
schema_upgrade_script_from() {
  local recorded="$1"
  if [[ "${recorded}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]] &&
    [ "${BASH_REMATCH[3]}" != "0" ]; then
    printf '%s.%s.0' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
    return 0
  fi
  printf '%s' "${recorded}"
}

upgrade_script_applies() {
  local current="$1"
  local from="$2"
  local baseline
  if [ "$from" = "$current" ]; then
    return 0
  fi
  baseline="$(schema_upgrade_script_from "$current")"
  [ "$from" = "$baseline" ] && [ "$from" != "$current" ]
}

find_next_upgrade() {
  local current="$1"
  local next_file="" next_to="" f base from to
  shopt -s nullglob
  local files=("${SCRIPT_DB_DIR}"/upgrade-*-to-*-"${DB_ENGINE}".sql)
  shopt -u nullglob
  [ ${#files[@]} -eq 0 ] && return 0
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    base=$(basename "$f")
    # upgrade-<from>-to-<to>-<engine>.sql, split on the last "-to-".
    base="${base#upgrade-}"
    base="${base%-"${DB_ENGINE}".sql}"
    from="${base%-to-*}"
    to="${base##*-to-}"
    if upgrade_script_applies "$current" "$from" && version_le "$to" "$VERSION"; then
      if [ -z "$next_to" ] || version_lt "$to" "$next_to"; then
        next_file="$f"
        next_to="$to"
      fi
    fi
  done < <(printf '%s\n' "${files[@]}" | sort -V)
  if [ -n "$next_file" ]; then
    printf '%s %s' "$next_file" "$next_to"
  fi
}

has_unreachable_later_script() {
  local current="$1"
  local f base from to
  shopt -s nullglob
  local files=("${SCRIPT_DB_DIR}"/upgrade-*-to-*-"${DB_ENGINE}".sql)
  shopt -u nullglob
  [ ${#files[@]} -eq 0 ] && return 1
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    base=$(basename "$f")
    base="${base#upgrade-}"
    base="${base%-"${DB_ENGINE}".sql}"
    from="${base%-to-*}"
    to="${base##*-to-}"
    if version_lt "$current" "$from" && version_le "$to" "$VERSION"; then
      return 0
    fi
  done < <(printf '%s\n' "${files[@]}" | sort -V)
  return 1
}

apply_upgrade_chain() {
  local current="$1"
  echo "Existing database detected. Recorded schema version: ${current}; target: ${VERSION}"
  if version_lt "$VERSION" "$current"; then
    echo "ERROR: Recorded schema version ${current} is newer than image version ${VERSION}" >&2
    exit 1
  fi
  if [ "$current" = "$VERSION" ]; then
    echo "Schema already at ${current}, skipping upgrade scripts."
    return 0
  fi
  while version_lt "$current" "$VERSION"; do
    local found next_file next_to
    found=$(find_next_upgrade "$current")
    if [ -z "$found" ]; then
      if has_unreachable_later_script "$current"; then
        echo "ERROR: No upgrade script from ${current} toward ${VERSION}, but later scripts exist." >&2
        echo "Refusing to skip intermediate migrations." >&2
        exit 1
      fi
      echo "INFO: No upgrade scripts between ${current} and ${VERSION}; checking column capacity before recording the patch version."
      return 0
    fi
    next_file=${found% *}
    next_to=${found##* }
    echo "Upgrade file: $next_file"
    apply_sql_file "$next_file"
    current="$next_to"
    write_recorded_version "$current"
  done
}

ensure_pg_schema() {
  [ "$DB_ENGINE" = postgresql ] || return 0
  [ -n "$PG_CURRENT_SCHEMA" ] || return 0
  local schema_exists
  schema_exists=$(psql_client -d "$GRAVITINO_DB" -v ON_ERROR_STOP=1 -tAc \
    "SELECT 1 FROM pg_namespace WHERE nspname = '${PG_CURRENT_SCHEMA}'")
  if [ "$schema_exists" != "1" ]; then
    psql_client -d "$GRAVITINO_DB" -v ON_ERROR_STOP=1 \
      -c "CREATE SCHEMA \"${PG_CURRENT_SCHEMA}\""
  fi
}

wait_for_db
ensure_pg_schema

VERSION=$(cat "$VERSION_FILE")
if [ -z "$VERSION" ]; then
  echo "ERROR: Please set VERSION (missing ${VERSION_FILE})" >&2
  exit 1
fi
require_semver "VERSION" "$VERSION"
if [ -n "$BASELINE_VERSION" ]; then
  require_semver "schema.baselineVersion / BASELINE_VERSION" "$BASELINE_VERSION"
fi

SCRIPT_DB_DIR="${SCRIPTS_DIR}/${DB_ENGINE}"
schema_file=$(select_schema_file)
if [ ! -f "$schema_file" ]; then
  echo "ERROR: Failed to find schema file for ${VERSION} under ${SCRIPT_DB_DIR}" >&2
  exit 1
fi
echo "Schema file: $schema_file"

ensure_migration_table

if ! metalake_exists; then
  echo "Fresh install detected, skipping upgrade script."
  apply_sql_file "$schema_file"
  write_recorded_version "$VERSION"
  echo "Schema migration completed successfully"
  exit 0
fi

current=$(read_recorded_version)
if [ -z "$current" ] && [ -n "$BASELINE_VERSION" ]; then
  echo "No recorded schema version; using schema.baselineVersion=${BASELINE_VERSION}"
  current="$BASELINE_VERSION"
  write_recorded_version "$current"
fi
if [ -z "$current" ]; then
  inferred=$(infer_schema_version)
  if [ -n "$inferred" ]; then
    echo "No recorded schema version; inferred ${inferred} from existing tables."
    current="$inferred"
    write_recorded_version "$current"
  fi
fi
if [ -z "$current" ]; then
  echo "ERROR: Existing database has no recorded schema version in gravitino_schema_migration." >&2
  echo "Refusing to guess which upgrade scripts to apply." >&2
  echo "Set schema.baselineVersion to the Gravitino version that created this database," >&2
  echo "or INSERT a row: gravitino_schema_migration id=1 schema_version='x.y.z'." >&2
  echo "If the database is already at ${VERSION}, set schema.baselineVersion=${VERSION}." >&2
  exit 1
fi

apply_upgrade_chain "$current"
ensure_column_name_capacity

# Snapshot schema-*.sql is for empty databases. Re-applying it on an existing
# catalog fails on PostgreSQL CREATE INDEX (no IF NOT EXISTS) even when
# CREATE TABLE IF NOT EXISTS is a no-op.
echo "Existing database: skipping schema snapshot after upgrades."
write_recorded_version "$VERSION"
echo "Schema migration completed successfully"
