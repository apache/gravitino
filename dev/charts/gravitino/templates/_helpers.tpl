{{/*
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
*/}}

{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "gravitino.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "gravitino.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "gravitino.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "gravitino.labels" -}}
app: {{ template "gravitino.name" . }}
chart: {{ template "gravitino.chart" . }}
release: {{ .Release.Name }}
{{- end }}

{{/*
Define the gravitino.namespace
*/}}
{{- define "gravitino.namespace" -}}
    {{- .Release.Namespace -}}
{{- end -}}

{{/*
Return an image reference for the provided image root and global values.
*/}}
{{- define "gravitino.imageReference" -}}
{{- $registry := coalesce .global.imageRegistry .imageRoot.registry -}}
{{- $repository := required "image repository is required" .imageRoot.repository -}}
{{- if .imageRoot.digest -}}
{{- if $registry -}}
{{- printf "%s/%s@%s" $registry $repository .imageRoot.digest -}}
{{- else -}}
{{- printf "%s@%s" $repository .imageRoot.digest -}}
{{- end -}}
{{- else -}}
{{- $tag := required "image tag is required" .imageRoot.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Return the proper Gravitino image name
*/}}
{{- define "gravitino.image" -}}
  {{- include "gravitino.imageReference" (dict "imageRoot" .Values.image "global" .Values.global) -}}
{{- end -}}

{{/*
Return the proper MySQL image name for the init-mysql initContainer
*/}}
{{- define "init-mysql.image" -}}
{{- include "gravitino.imageReference" (dict "imageRoot" .Values.mysql.image "global" .Values.global) -}}
{{- end -}}

{{/*
Return the proper Postgresql image name for the init-postgresql initContainer
*/}}
{{- define "init-postgresql.image" -}}
{{- include "gravitino.imageReference" (dict "imageRoot" .Values.postgresql.image "global" .Values.global) -}}
{{- end -}}

{{/*
Bitnami MySQL subchart resource name (Service / StatefulSet / default Secret).
Matches common.names.fullname: when the release name already contains the chart
name ("mysql"), Bitnami uses the release name alone — not Release.Name-mysql.
*/}}
{{- define "gravitino.mysql.fullname" -}}
{{- $mysql := .Values.mysql | default dict -}}
{{- if $mysql.fullnameOverride -}}
{{- $mysql.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "mysql" $mysql.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Bitnami MySQL auth Secret name (existingSecret or chart-managed fullname).
*/}}
{{- define "gravitino.mysql.secretName" -}}
{{- $mysql := .Values.mysql | default dict -}}
{{- $auth := $mysql.auth | default dict -}}
{{- if $auth.existingSecret -}}
{{- tpl ($auth.existingSecret | toString) . -}}
{{- else -}}
{{- include "gravitino.mysql.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
Bitnami PostgreSQL subchart resource name (Service / StatefulSet / default Secret).
Matches common.names.fullname collapse when the release name contains "postgresql".
*/}}
{{- define "gravitino.postgresql.fullname" -}}
{{- $postgresql := .Values.postgresql | default dict -}}
{{- if $postgresql.fullnameOverride -}}
{{- $postgresql.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "postgresql" $postgresql.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Bitnami PostgreSQL auth Secret name (existingSecret or chart-managed fullname).
*/}}
{{- define "gravitino.postgresql.secretName" -}}
{{- $postgresql := .Values.postgresql | default dict -}}
{{- $auth := $postgresql.auth | default dict -}}
{{- if $auth.existingSecret -}}
{{- tpl ($auth.existingSecret | toString) . -}}
{{- else -}}
{{- include "gravitino.postgresql.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
True when per-pod init-mysql should apply schema SQL (in-chart Bitnami only).
Skipped when schema.migrate.enabled runs the shared Job instead.
*/}}
{{- define "gravitino.mysqlSchemaInitEnabled" -}}
{{- if and .Values.mysql.enabled (ne (include "gravitino.schemaMigrateEnabled" . | trim) "true") -}}
true
{{- end -}}
{{- end -}}

{{/*
True when per-pod init-postgresql should apply schema SQL (in-chart Bitnami only).
Skipped when schema.migrate.enabled runs the shared Job instead.
*/}}
{{- define "gravitino.postgresqlSchemaInitEnabled" -}}
{{- if and .Values.postgresql.enabled (ne (include "gravitino.schemaMigrateEnabled" . | trim) "true") -}}
true
{{- end -}}
{{- end -}}

{{/*
True when Deployment should wait for gravitino_schema_migration (schema-migrate Job).
*/}}
{{- define "gravitino.schemaWaitEnabled" -}}
{{- if eq (include "gravitino.schemaMigrateEnabled" . | trim) "true" -}}
true
{{- end -}}
{{- end -}}

{{/*
How long wait-for-schema init containers poll before failing.
*/}}
{{- define "gravitino.schemaWaitTimeoutSeconds" -}}
{{- $schema := .Values.schema | default dict -}}
{{- $wait := $schema.wait | default dict -}}
{{- $n := int ($wait.timeoutSeconds | default 600) -}}
{{- if lt $n 1 -}}
{{- fail "schema.wait.timeoutSeconds must be a positive integer" -}}
{{- end -}}
{{- $n -}}
{{- end -}}

{{/*
Shared prefix for wait-for-schema init containers.
*/}}
{{- define "gravitino.schemaWaitPreamble" -}}
timeout="${SCHEMA_WAIT_TIMEOUT:-600}"
case "$timeout" in
  ''|*[!0-9]*) echo "ERROR: SCHEMA_WAIT_TIMEOUT must be a positive integer" >&2; exit 1 ;;
esac
if [ "$timeout" -lt 1 ]; then
  echo "ERROR: SCHEMA_WAIT_TIMEOUT must be a positive integer" >&2
  exit 1
fi
deadline=$((SECONDS + timeout))
required_column_name_capacity=128
if [ "$(printf '%s\n%s' 1.3.2 "$VERSION" | sort -V | head -n1)" = 1.3.2 ]; then
  required_column_name_capacity=300
fi
{{- end -}}

{{/*
Schema readiness includes the 1.3.2+ column expansion.
*/}}
{{- define "gravitino.schemaReadyQuery" -}}
SELECT schema_version FROM gravitino_schema_migration WHERE id = 1 AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = {{ if eq . "mysql" }}DATABASE(){{ else }}current_schema(){{ end }} AND table_name = 'table_column_version_info' AND column_name = 'column_name' AND character_maximum_length >= ${required_column_name_capacity});
{{- end -}}

{{/*
True when the entity store uses an external PostgreSQL JDBC URL (not in-chart Bitnami).
*/}}
{{- define "gravitino.entityUsesExternalPostgresql" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- if and (not .Values.mysql.enabled) (not .Values.postgresql.enabled) (hasPrefix "jdbc:postgresql:" $url) -}}
true
{{- end -}}
{{- end -}}

{{/*
Authority segment (host[:port]) from entity.jdbcUrl for PostgreSQL.

Only a single host:port is supported. The helpers split authority on the first
colon and do not parse IPv6 bracket literals (e.g. [::1]:5432) or multi-host
JDBC authorities (host1:5432,host2:5432). When the authority contains "[" or
"," and host/port would be derived from the URL, rendering fails and asks for
postgresql.init.host and postgresql.init.port.
*/}}
{{- define "gravitino.entityJdbcAuthorityFromUrl" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- if hasPrefix "jdbc:postgresql://" $url -}}
{{- $path := trimPrefix "jdbc:postgresql://" $url -}}
{{- $auth := (splitList "/" $path | first) -}}
{{- if or (contains "[" $auth) (contains "," $auth) -}}
{{- fail "entity.jdbcUrl authority is IPv6 or multi-host; set schema.dbHost and schema.dbPort explicitly (JDBC URL host/port fallback supports only a single host:port)" -}}
{{- end -}}
{{- $auth -}}
{{- end -}}
{{- end -}}

{{/*
Hostname from entity.jdbcUrl authority (external PostgreSQL).

Supports a single hostname or IPv4 host only (see entityJdbcAuthorityFromUrl).
*/}}
{{- define "gravitino.entityJdbcHostFromUrl" -}}
{{- $auth := include "gravitino.entityJdbcAuthorityFromUrl" . | trim -}}
{{- if $auth -}}
{{- if contains ":" $auth -}}
{{- $parts := splitList ":" $auth -}}
{{- index $parts 0 -}}
{{- else -}}
{{- $auth -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Port from entity.jdbcUrl authority (defaults to 5432 when omitted).

Supports a single host:port only (see entityJdbcAuthorityFromUrl).
*/}}
{{- define "gravitino.entityJdbcPortFromUrl" -}}
{{- $auth := include "gravitino.entityJdbcAuthorityFromUrl" . | trim -}}
{{- if and $auth (contains ":" $auth) -}}
{{- $parts := splitList ":" $auth -}}
{{- index $parts 1 -}}
{{- else -}}
5432
{{- end -}}
{{- end -}}

{{/*
sslmode query parameter from entity.jdbcUrl.

When sslmode is absent, JDBC ssl=true (or ssl=1) implies certificate and hostname
verification, aligned with the PostgreSQL JDBC driver — mapped to verify-full for psql.
*/}}
{{- define "gravitino.entityJdbcSslModeFromUrl" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- $match := regexFind "sslmode=([^&]+)" $url -}}
{{- if $match -}}
{{- trimPrefix "sslmode=" $match -}}
{{- else if or (regexFind "[?&]ssl=true(&|$)" $url) (regexFind "[?&]ssl=1(&|$)" $url) -}}
verify-full
{{- end -}}
{{- end -}}

{{/*
currentSchema query parameter from entity.jdbcUrl.
*/}}
{{- define "gravitino.entityJdbcCurrentSchemaFromUrl" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- $match := regexFind "currentSchema=([^&]+)" $url -}}
{{- if $match -}}
{{- trimPrefix "currentSchema=" $match -}}
{{- end -}}
{{- end -}}

{{/*
Database name segment from entity.jdbcUrl (postgresql JDBC URLs only).
*/}}
{{- define "gravitino.entityJdbcDatabaseFromUrl" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- if hasPrefix "jdbc:postgresql://" $url -}}
{{- $path := trimPrefix "jdbc:postgresql://" $url -}}
{{- $parts := splitList "/" $path -}}
{{- if ge (len $parts) 2 -}}
{{- $dbPart := index $parts 1 -}}
{{- $db := splitList "?" $dbPart | first -}}
{{- if $db -}}
{{- $db -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
True when the entity store uses an external MySQL JDBC URL (not in-chart Bitnami).
*/}}
{{- define "gravitino.entityUsesExternalMysql" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- if and (not .Values.mysql.enabled) (not .Values.postgresql.enabled) (hasPrefix "jdbc:mysql://" $url) -}}
true
{{- end -}}
{{- end -}}

{{/*
True when a relational JDBC backend is configured for schema migration.
*/}}
{{- define "gravitino.schemaHasTarget" -}}
{{- if or .Values.mysql.enabled .Values.postgresql.enabled (eq (include "gravitino.entityUsesExternalMysql" . | trim) "true") (eq (include "gravitino.entityUsesExternalPostgresql" . | trim) "true") -}}
true
{{- end -}}
{{- end -}}

{{/*
True when the schema-migrate Job should run (regular resource, not a Helm hook).
*/}}
{{- define "gravitino.schemaMigrateEnabled" -}}
{{- if .Values.schema.migrate.enabled -}}
{{- if ne (include "gravitino.schemaHasTarget" . | trim) "true" -}}
{{- fail "schema.migrate.enabled is true but no relational target: enable mysql/postgresql or set entity.jdbcUrl to jdbc:mysql:// or jdbc:postgresql://" -}}
{{- end -}}
true
{{- end -}}
{{- end -}}

{{/*
mysql or postgresql for schema migrate.
*/}}
{{- define "gravitino.schemaMigrateEngine" -}}
{{- if .Values.mysql.enabled -}}
mysql
{{- else if .Values.postgresql.enabled -}}
postgresql
{{- else if eq (include "gravitino.entityUsesExternalMysql" . | trim) "true" -}}
mysql
{{- else if eq (include "gravitino.entityUsesExternalPostgresql" . | trim) "true" -}}
postgresql
{{- end -}}
{{- end -}}

{{/*
Authority segment from jdbc:mysql:// URLs (single host:port only).
*/}}
{{- define "gravitino.entityJdbcMysqlAuthorityFromUrl" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- if hasPrefix "jdbc:mysql://" $url -}}
{{- $path := trimPrefix "jdbc:mysql://" $url -}}
{{- $path = splitList "?" $path | first -}}
{{- if contains "@" $path -}}
{{- $path = last (splitList "@" $path) -}}
{{- end -}}
{{- $auth := splitList "/" $path | first -}}
{{- if or (contains "[" $auth) (contains "," $auth) -}}
{{- fail "entity.jdbcUrl authority is IPv6 or multi-host; set schema.dbHost and schema.dbPort explicitly" -}}
{{- end -}}
{{- $auth -}}
{{- end -}}
{{- end -}}

{{/*
Database name from jdbc:mysql://host:port/db
*/}}
{{- define "gravitino.entityJdbcMysqlDatabaseFromUrl" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- if hasPrefix "jdbc:mysql://" $url -}}
{{- $path := trimPrefix "jdbc:mysql://" $url -}}
{{- $parts := splitList "/" $path -}}
{{- if ge (len $parts) 2 -}}
{{- $dbPart := index $parts 1 -}}
{{- $db := splitList "?" $dbPart | first -}}
{{- if $db -}}
{{- $db -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Host for schema-migrate Job.
*/}}
{{- define "gravitino.schemaMigrateHost" -}}
{{- $host := .Values.schema.dbHost | default "" | toString | trim -}}
{{- if $host -}}
{{- $host -}}
{{- else if .Values.mysql.enabled -}}
{{- include "gravitino.mysql.fullname" . -}}
{{- else if .Values.postgresql.enabled -}}
{{- include "gravitino.postgresql.fullname" . -}}
{{- else if eq (include "gravitino.entityUsesExternalMysql" . | trim) "true" -}}
{{- $auth := include "gravitino.entityJdbcMysqlAuthorityFromUrl" . | trim -}}
{{- if contains ":" $auth -}}
{{- index (splitList ":" $auth) 0 -}}
{{- else -}}
{{- $auth -}}
{{- end -}}
{{- else -}}
{{- $fromUrl := include "gravitino.entityJdbcHostFromUrl" . | trim -}}
{{- if $fromUrl -}}
{{- $fromUrl -}}
{{- else -}}
{{- fail "could not parse host from entity.jdbcUrl; set schema.dbHost" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Port for schema-migrate Job.
*/}}
{{- define "gravitino.schemaMigratePort" -}}
{{- $port := .Values.schema.dbPort | default "" | toString | trim -}}
{{- if $port -}}
{{- $port -}}
{{- else if .Values.mysql.enabled -}}
3306
{{- else if .Values.postgresql.enabled -}}
5432
{{- else if eq (include "gravitino.entityUsesExternalMysql" . | trim) "true" -}}
{{- $auth := include "gravitino.entityJdbcMysqlAuthorityFromUrl" . | trim -}}
{{- if and $auth (contains ":" $auth) -}}
{{- index (splitList ":" $auth) 1 -}}
{{- else -}}
3306
{{- end -}}
{{- else -}}
{{- include "gravitino.entityJdbcPortFromUrl" . | trim -}}
{{- end -}}
{{- end -}}

{{/*
Database name for schema-migrate Job.
*/}}
{{- define "gravitino.schemaMigrateDatabase" -}}
{{- $db := .Values.schema.dbName | default "" | toString | trim -}}
{{- if $db -}}
{{- $db -}}
{{- else if .Values.mysql.enabled -}}
{{- .Values.mysql.auth.database -}}
{{- else if .Values.postgresql.enabled -}}
{{- .Values.postgresql.auth.database | default "gravitino" -}}
{{- else if eq (include "gravitino.entityUsesExternalMysql" . | trim) "true" -}}
{{- $fromUrl := include "gravitino.entityJdbcMysqlDatabaseFromUrl" . | trim -}}
{{- if $fromUrl -}}
{{- $fromUrl -}}
{{- else -}}
{{- fail "could not parse database name from entity.jdbcUrl; use jdbc:mysql://<host>:<port>/<database> or set schema.dbName" -}}
{{- end -}}
{{- else -}}
{{- $fromUrl := include "gravitino.entityJdbcDatabaseFromUrl" . | trim -}}
{{- if $fromUrl -}}
{{- $fromUrl -}}
{{- else -}}
{{- fail "could not parse database name from entity.jdbcUrl; use jdbc:postgresql://<host>:<port>/<database> or set schema.dbName" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Password Secret for external relational stores (schema-migrate Job).
*/}}
{{- define "gravitino.schemaMigrateSecretName" -}}
{{- if or .Values.mysql.enabled .Values.postgresql.enabled -}}
{{- else if .Values.schema.migrate.existingSecret -}}
{{- .Values.schema.migrate.existingSecret -}}
{{- else -}}
{{- fail "schema.migrate.existingSecret is required when schema.migrate.enabled is true for external MySQL or PostgreSQL" -}}
{{- end -}}
{{- end -}}

{{/*
PostgreSQL currentSchema for schema-migrate Job (PG_CURRENT_SCHEMA / PGOPTIONS).
*/}}
{{- define "gravitino.schemaMigrateCurrentSchema" -}}
{{- $schema := "" -}}
{{- if .Values.schema.currentSchema -}}
{{- $schema = .Values.schema.currentSchema | toString | trim -}}
{{- else if eq (include "gravitino.entityUsesExternalPostgresql" . | trim) "true" -}}
{{- $schema = include "gravitino.entityJdbcCurrentSchemaFromUrl" . | trim -}}
{{- end -}}
{{- if $schema -}}
{{- if not (regexMatch "^[A-Za-z_][A-Za-z0-9_]*$" $schema) -}}
{{- fail (printf "schema.currentSchema / entity.jdbcUrl currentSchema %q must be a simple PostgreSQL identifier" $schema) -}}
{{- end -}}
{{- $schema -}}
{{- end -}}
{{- end -}}

{{/*
sslmode for schema-migrate Job (postgresql).
*/}}
{{- define "gravitino.schemaMigrateSslMode" -}}
{{- if .Values.schema.sslMode -}}
{{- .Values.schema.sslMode -}}
{{- else if eq (include "gravitino.entityUsesExternalPostgresql" . | trim) "true" -}}
{{- include "gravitino.entityJdbcSslModeFromUrl" . | trim -}}
{{- end -}}
{{- end -}}

{{- define "gravitino.schemaMigrateSslCaSecretName" -}}
{{- $ssl := .Values.schema.ssl | default dict -}}
{{- $secret := $ssl.existingSecret | default "" | toString | trim -}}
{{- if $secret -}}
{{- $secret -}}
{{- end -}}
{{- end -}}

{{- define "gravitino.schemaMigrateSslCaKey" -}}
{{- $ssl := .Values.schema.ssl | default dict -}}
{{- $ssl.caCert | default "ca.crt" | toString | trim -}}
{{- end -}}

{{- define "gravitino.schemaMigrateSslValidate" -}}
{{- if ne (include "gravitino.schemaMigrateEngine" . | trim) "postgresql" -}}
{{- else -}}
{{- $mode := include "gravitino.schemaMigrateSslMode" . | trim | lower -}}
{{- $caSecret := include "gravitino.schemaMigrateSslCaSecretName" . | trim -}}
{{- if and (or (eq $mode "verify-ca") (eq $mode "verify-full")) (not $caSecret) -}}
{{- fail "schema.ssl.existingSecret is required when sslmode is verify-ca or verify-full for the schema-migrate Job" -}}
{{- end -}}
{{- end -}}
{{- end -}}