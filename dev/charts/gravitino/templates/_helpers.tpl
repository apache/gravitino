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
{{- if .Values.mysql.enabled -}}
  {{- include "gravitino.imageReference" (dict "imageRoot" .Values.mysql.image "global" .Values.global) -}}
{{- end -}}
{{- end -}}

{{/*
Return the proper Postgresql image name for the init-postgresql initContainer
*/}}
{{- define "init-postgresql.image" -}}
{{- include "gravitino.imageReference" (dict "imageRoot" .Values.postgresql.image "global" .Values.global) -}}
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
True when the init-postgresql schema init container should be rendered.
In-chart Bitnami PostgreSQL always runs schema init. External PostgreSQL runs
only when postgresql.init.enabled is true (opt-in; see issue #9013 for hook Job).
*/}}
{{- define "gravitino.postgresqlSchemaInitEnabled" -}}
{{- if .Values.postgresql.enabled -}}
true
{{- else if and (eq (include "gravitino.entityUsesExternalPostgresql" . | trim) "true") .Values.postgresql.init.enabled -}}
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
{{- fail "entity.jdbcUrl authority is IPv6 or multi-host; set postgresql.init.host and postgresql.init.port explicitly (JDBC URL host/port fallback supports only a single host:port)" -}}
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
*/}}
{{- define "gravitino.entityJdbcSslModeFromUrl" -}}
{{- $url := .Values.entity.jdbcUrl | default "" -}}
{{- $match := regexFind "sslmode=([^&]+)" $url -}}
{{- if $match -}}
{{- trimPrefix "sslmode=" $match -}}
{{- end -}}
{{- end -}}

{{/*
Hostname for init-postgresql psql (-h).
*/}}
{{- define "gravitino.postgresqlInitHost" -}}
{{- if .Values.postgresql.init.host -}}
{{- .Values.postgresql.init.host -}}
{{- else if .Values.postgresql.enabled -}}
{{- printf "%s-postgresql" .Release.Name -}}
{{- else -}}
{{- $fromUrl := include "gravitino.entityJdbcHostFromUrl" . | trim -}}
{{- if $fromUrl -}}
{{- $fromUrl -}}
{{- else -}}
{{- fail "postgresql.init.host is required (or set a parseable entity.jdbcUrl host) when postgresql.init.enabled is true for external PostgreSQL" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Port for init-postgresql (PGPORT).
*/}}
{{- define "gravitino.postgresqlInitPort" -}}
{{- if .Values.postgresql.init.port -}}
{{- .Values.postgresql.init.port | toString -}}
{{- else if .Values.postgresql.enabled -}}
5432
{{- else -}}
{{- include "gravitino.entityJdbcPortFromUrl" . | trim -}}
{{- end -}}
{{- end -}}

{{/*
sslmode for init-postgresql (PGSSLMODE).
*/}}
{{- define "gravitino.postgresqlInitSslMode" -}}
{{- if .Values.postgresql.init.sslMode -}}
{{- .Values.postgresql.init.sslMode -}}
{{- else -}}
{{- include "gravitino.entityJdbcSslModeFromUrl" . | trim -}}
{{- end -}}
{{- end -}}

{{/*
Secret name for init-postgresql POSTGRES_PASSWORD secretKeyRef.
*/}}
{{- define "gravitino.postgresqlInitSecretName" -}}
{{- if .Values.postgresql.init.existingSecret -}}
{{- .Values.postgresql.init.existingSecret -}}
{{- else if and .Values.postgresql.enabled .Values.postgresql.auth.existingSecret -}}
{{- .Values.postgresql.auth.existingSecret -}}
{{- else if .Values.postgresql.enabled -}}
{{- printf "%s-postgresql" .Release.Name -}}
{{- else if eq (include "gravitino.entityUsesExternalPostgresql" . | trim) "true" -}}
{{- fail "postgresql.init.existingSecret is required when postgresql.init.enabled is true for external PostgreSQL" -}}
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
Application user for init-postgresql POSTGRES_USER.
*/}}
{{- define "gravitino.postgresqlInitUsername" -}}
{{- if .Values.postgresql.enabled -}}
{{- .Values.postgresql.auth.username -}}
{{- else -}}
{{- .Values.entity.jdbcUser -}}
{{- end -}}
{{- end -}}

{{/*
Database name for init-postgresql GRAVITINO_DB.
*/}}
{{- define "gravitino.postgresqlInitDatabase" -}}
{{- if eq (include "gravitino.entityUsesExternalPostgresql" . | trim) "true" -}}
{{- $fromUrl := include "gravitino.entityJdbcDatabaseFromUrl" . | trim -}}
{{- if $fromUrl -}}
{{- $fromUrl -}}
{{- else -}}
{{- fail "could not parse database name from entity.jdbcUrl for init-postgresql; use jdbc:postgresql://<host>:<port>/<database>" -}}
{{- end -}}
{{- else -}}
{{- .Values.postgresql.auth.database | default "gravitino" -}}
{{- end -}}
{{- end -}}