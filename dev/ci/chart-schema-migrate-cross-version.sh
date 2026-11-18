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
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Live kind coverage: seed an existing MySQL schema at FROM_VERSION, then helm
# upgrade with schema.migrate.enabled so the Job applies the full upgrade chain
# to the image version (for example 1.2.0 -> 1.3.0 -> 2.0.0).
#
# Required env:
#   KIND_REGISTRY, tag (image tag built by chart-test)
# Optional env:
#   NAMESPACE, RELEASE_NAME, FROM_VERSION, CHART_DIR, VALUES_FILE, REPO_ROOT
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
CHART_DIR="${CHART_DIR:-${REPO_ROOT}/dev/charts/gravitino}"
VALUES_FILE="${VALUES_FILE:-${REPO_ROOT}/dev/charts/gravitino/resources/scenarios/schema-migrate-mysql.yaml}"
NAMESPACE="${NAMESPACE:-schema-migrate-cross}"
RELEASE_NAME="${RELEASE_NAME:-gravitino-sm-cross}"
FROM_VERSION="${FROM_VERSION:-1.2.0}"
CHECK_SCRIPT="${REPO_ROOT}/dev/ci/check-schema-migrate.sh"
SCHEMA_SQL="${REPO_ROOT}/scripts/mysql/schema-${FROM_VERSION}-mysql.sql"

: "${KIND_REGISTRY:?KIND_REGISTRY is required}"
: "${tag:?tag is required}"

if [ ! -f "$SCHEMA_SQL" ]; then
  echo "ERROR: missing baseline schema SQL: ${SCHEMA_SQL}" >&2
  exit 1
fi
if [ ! -f "$CHECK_SCRIPT" ]; then
  echo "ERROR: missing ${CHECK_SCRIPT}" >&2
  exit 1
fi

chmod +x "$CHECK_SCRIPT"
helm dependency update "$CHART_DIR"

cleanup() {
  helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" --wait || true
  kubectl delete namespace "$NAMESPACE" --wait=false || true
}
trap cleanup EXIT

kubectl create namespace "$NAMESPACE"

echo "==> Fresh install with schema.migrate.enabled (brings MySQL up)"
helm upgrade --install "$RELEASE_NAME" "$CHART_DIR" -n "$NAMESPACE" --wait --timeout 10m \
  --values "$VALUES_FILE" \
  --set image.registry="" \
  --set image.repository="${KIND_REGISTRY}/gravitino" \
  --set image.tag="${tag}"
bash "$CHECK_SCRIPT" "$NAMESPACE" "$RELEASE_NAME" mysql

DEPLOY=$(kubectl get deploy -n "$NAMESPACE" -l "release=${RELEASE_NAME}" -o jsonpath='{.items[0].metadata.name}')
DB_POD=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=mysql" \
  -o jsonpath='{.items[0].metadata.name}')
DB_NAME=$(kubectl get deploy -n "$NAMESPACE" "$DEPLOY" \
  -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="init-mysql")].env[?(@.name=="GRAVITINO_DB")].value}')
ROOT_PW=$(kubectl get secret -n "$NAMESPACE" "${RELEASE_NAME}-mysql" \
  -o jsonpath='{.data.mysql-root-password}' | base64 -d)

echo "==> Scale Gravitino to zero before reseeding ${FROM_VERSION} schema"
kubectl scale deploy -n "$NAMESPACE" "$DEPLOY" --replicas=0
kubectl wait -n "$NAMESPACE" --for=delete pod -l "release=${RELEASE_NAME}" --timeout=180s || true

echo "==> Reset database to schema-${FROM_VERSION}-mysql.sql (no gravitino_schema_migration row)"
kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
  mysql -uroot -p"${ROOT_PW}" -e "DROP DATABASE IF EXISTS \`${DB_NAME}\`; CREATE DATABASE \`${DB_NAME}\`;"
kubectl cp "$SCHEMA_SQL" "${NAMESPACE}/${DB_POD}:/tmp/schema-${FROM_VERSION}-mysql.sql"
kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
  bash -c "mysql -uroot -p\"${ROOT_PW}\" -D '${DB_NAME}' < /tmp/schema-${FROM_VERSION}-mysql.sql"

HAS_AUDIT=$(kubectl exec -n "$NAMESPACE" "$DB_POD" -- \
  mysql -uroot -p"${ROOT_PW}" -D "${DB_NAME}" -sNe \
  "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='view_meta' AND column_name='audit_info'")
HAS_AUDIT=$(echo "$HAS_AUDIT" | tr -d '[:space:]')
if [ "$HAS_AUDIT" != "0" ]; then
  echo "ERROR: expected view_meta.audit_info to be absent on ${FROM_VERSION} schema" >&2
  exit 1
fi
echo "Seeded ${FROM_VERSION}: view_meta.audit_info is absent (as expected)"

echo "==> Helm upgrade with schema.baselineVersion=${FROM_VERSION} (cross-version migrate)"
helm upgrade "$RELEASE_NAME" "$CHART_DIR" -n "$NAMESPACE" --wait --timeout 10m \
  --values "$VALUES_FILE" \
  --set image.registry="" \
  --set image.repository="${KIND_REGISTRY}/gravitino" \
  --set image.tag="${tag}" \
  --set "schema.baselineVersion=${FROM_VERSION}" \
  --set "podAnnotations.schema-migrate-cross=$(date +%s)"

bash "$CHECK_SCRIPT" "$NAMESPACE" "$RELEASE_NAME" mysql

JOB=$(kubectl get jobs -n "$NAMESPACE" -l app.kubernetes.io/component=schema-migrate \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}')
LOG="/tmp/${JOB}-cross.log"
kubectl logs -n "$NAMESPACE" "job/${JOB}" --all-containers=true | tee "$LOG"
grep -q "upgrade-${FROM_VERSION}-to-1.3.0-mysql" "$LOG"
grep -q "upgrade-1.3.0-to-2.0.0-mysql" "$LOG"
grep -q "schema.baselineVersion=${FROM_VERSION}" "$LOG"

echo "Cross-version schema migrate (${FROM_VERSION} -> image) passed"
