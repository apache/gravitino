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

# Entity-store schema init for the Helm schema-migrate hook Job.
set -euo pipefail

: "${DB_ENGINE:?DB_ENGINE must be mysql or postgresql}"
: "${DB_HOST:?DB_HOST is required}"
: "${GRAVITINO_DB:?GRAVITINO_DB is required}"
: "${SCRIPTS_DIR:=/scripts}"

if [ "$DB_ENGINE" != mysql ] && [ "$DB_ENGINE" != postgresql ]; then
  echo "ERROR: DB_ENGINE must be mysql or postgresql (got ${DB_ENGINE})" >&2
  exit 1
fi

DB_PORT="${DB_PORT:-}"
PG_CURRENT_SCHEMA="${PG_CURRENT_SCHEMA:-}"

wait_for_db() {
  echo "Waiting for ${DB_ENGINE} at ${DB_HOST}..."
  if [ "$DB_ENGINE" = mysql ]; then
    until mysql -h "$DB_HOST" "${mysql_extra[@]}" -u "$MYSQL_USER" -e "SELECT 1" >/dev/null 2>&1; do
      echo "Waiting for MySQL to be ready..."
      sleep 2
    done
  else
    until PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -U "$POSTGRES_USER" -d "$GRAVITINO_DB" -c "SELECT 1" >/dev/null 2>&1; do
      echo "Waiting for PostgreSQL to be ready..."
      sleep 2
    done
  fi
}

mysql_extra=()
if [ -n "$DB_PORT" ]; then
  mysql_extra+=(-P "$DB_PORT")
fi

apply_migrations() {
  local version dir schema_file upgrade_file
  version=$(cat "${SCRIPTS_DIR}/version.txt")
  if [ -z "$version" ]; then
    echo "ERROR: version.txt is empty" >&2
    exit 1
  fi
  dir="${SCRIPTS_DIR}/${DB_ENGINE}"
  schema_file=$(ls "$dir"/schema-*-"${DB_ENGINE}".sql | sort -V | awk -F'-' -v ver="$version" '$2 <= ver {print $0}' | tail -n 1)
  if [ ! -f "$schema_file" ]; then
    echo "ERROR: Failed to find schema file under $dir for version $version" >&2
    exit 1
  fi
  upgrade_file=$(ls "$dir"/upgrade-*-to-*-"${DB_ENGINE}".sql | sort -V | awk -F'-' -v ver="$version" '$4 <= ver {print $0}' | tail -n 1)
  if [ ! -f "$upgrade_file" ]; then
    echo "ERROR: Failed to find upgrade file under $dir for version $version" >&2
    exit 1
  fi
  echo "Schema file: $schema_file"
  echo "Upgrade file: $upgrade_file"

  if [ "$DB_ENGINE" = mysql ]; then
    if ! mysql -h "$DB_HOST" "${mysql_extra[@]}" -u "$MYSQL_USER" -D "$GRAVITINO_DB" <"$upgrade_file"; then
      echo "WARNING: Upgrade script failed: $upgrade_file" >&2
      echo "Continuing with schema script..." >&2
    fi
    mysql -h "$DB_HOST" "${mysql_extra[@]}" -u "$MYSQL_USER" -D "$GRAVITINO_DB" <"$schema_file"
  else
    if [ -n "$PG_CURRENT_SCHEMA" ]; then
      schema_exists=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -U "$POSTGRES_USER" -d "$GRAVITINO_DB" -v ON_ERROR_STOP=1 -tAc \
        "SELECT 1 FROM pg_namespace WHERE nspname = '${PG_CURRENT_SCHEMA}'")
      if [ "$schema_exists" != "1" ]; then
        PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -U "$POSTGRES_USER" -d "$GRAVITINO_DB" -v ON_ERROR_STOP=1 \
          -c "CREATE SCHEMA \"${PG_CURRENT_SCHEMA}\""
      fi
    fi
    if ! PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -U "$POSTGRES_USER" -d "$GRAVITINO_DB" -f "$upgrade_file"; then
      echo "WARNING: Upgrade script failed: $upgrade_file" >&2
      echo "Continuing with schema script..." >&2
    fi
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -U "$POSTGRES_USER" -d "$GRAVITINO_DB" -v ON_ERROR_STOP=1 -f "$schema_file"
  fi
}

if [ "$DB_ENGINE" = mysql ]; then
  : "${MYSQL_USER:?MYSQL_USER is required for mysql}"
  : "${MYSQL_PWD:?MYSQL_PWD is required for mysql}"
  export MYSQL_PWD
else
  : "${POSTGRES_USER:?POSTGRES_USER is required for postgresql}"
  : "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required for postgresql}"
fi

wait_for_db
apply_migrations
echo "Schema migration completed successfully"