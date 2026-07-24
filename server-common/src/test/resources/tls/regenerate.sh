#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.


set -euo pipefail

STORE_PASSWORD="changeit"
KEY_PASSWORD="changeit"
STORE_TYPE="PKCS12"
VALIDITY_DAYS="3650"
KEY_ALGORITHM="RSA"
KEY_SIZE="2048"
SIGNATURE_ALGORITHM="SHA256withRSA"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

SERVER_KEYSTORE="test-server-keystore.p12"
SERVER_TRUSTSTORE="test-server-truststore.p12"
CLIENT_TRUSTSTORE="test-client-truststore.p12"
TRUSTED_CLIENT_KEYSTORE="test-trusted-client-keystore.p12"
UNTRUSTED_CLIENT_KEYSTORE="test-untrusted-client-keystore.p12"
UNTRUSTED_SERVER_TRUSTSTORE="test-untrusted-server-truststore.p12"

SERVER_ALIAS="test-server"
TRUSTED_CLIENT_ALIAS="test-trusted-client"
UNTRUSTED_CLIENT_ALIAS="test-untrusted-client"
UNRELATED_SERVER_ALIAS="test-unrelated-server"

SERVER_SUBJECT="CN=localhost,OU=TLS Integration Tests,O=Apache Gravitino,L=Test,ST=Test,C=US"
TRUSTED_CLIENT_SUBJECT="CN=test-trusted-client,OU=TLS Integration Tests,O=Apache Gravitino,L=Test,ST=Test,C=US"
UNTRUSTED_CLIENT_SUBJECT="CN=test-untrusted-client,OU=TLS Integration Tests,O=Apache Gravitino,L=Test,ST=Test,C=US"
UNRELATED_SERVER_SUBJECT="CN=test-unrelated-server,OU=TLS Integration Tests,O=Apache Gravitino,L=Test,ST=Test,C=US"

TEMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "${TEMP_DIR}"
}
trap cleanup EXIT

if ! command -v keytool >/dev/null 2>&1; then
  echo "Error: keytool was not found on PATH." >&2
  echo "Install a JDK or add its bin directory to PATH." >&2
  exit 1
fi

echo "Removing existing TLS test fixtures..."

rm -f \
  "${SERVER_KEYSTORE}" \
  "${SERVER_TRUSTSTORE}" \
  "${CLIENT_TRUSTSTORE}" \
  "${TRUSTED_CLIENT_KEYSTORE}" \
  "${UNTRUSTED_CLIENT_KEYSTORE}" \
  "${UNTRUSTED_SERVER_TRUSTSTORE}"

echo "Generating server identity..."

keytool -genkeypair \
  -alias "${SERVER_ALIAS}" \
  -dname "${SERVER_SUBJECT}" \
  -ext "SAN=dns:localhost" \
  -ext "EKU=serverAuth" \
  -keyalg "${KEY_ALGORITHM}" \
  -keysize "${KEY_SIZE}" \
  -sigalg "${SIGNATURE_ALGORITHM}" \
  -validity "${VALIDITY_DAYS}" \
  -storetype "${STORE_TYPE}" \
  -keystore "${SERVER_KEYSTORE}" \
  -storepass "${STORE_PASSWORD}" \
  -keypass "${KEY_PASSWORD}" \
  -noprompt

echo "Generating trusted client identity..."

keytool -genkeypair \
  -alias "${TRUSTED_CLIENT_ALIAS}" \
  -dname "${TRUSTED_CLIENT_SUBJECT}" \
  -ext "EKU=clientAuth" \
  -keyalg "${KEY_ALGORITHM}" \
  -keysize "${KEY_SIZE}" \
  -sigalg "${SIGNATURE_ALGORITHM}" \
  -validity "${VALIDITY_DAYS}" \
  -storetype "${STORE_TYPE}" \
  -keystore "${TRUSTED_CLIENT_KEYSTORE}" \
  -storepass "${STORE_PASSWORD}" \
  -keypass "${KEY_PASSWORD}" \
  -noprompt

echo "Generating untrusted client identity..."

keytool -genkeypair \
  -alias "${UNTRUSTED_CLIENT_ALIAS}" \
  -dname "${UNTRUSTED_CLIENT_SUBJECT}" \
  -ext "EKU=clientAuth" \
  -keyalg "${KEY_ALGORITHM}" \
  -keysize "${KEY_SIZE}" \
  -sigalg "${SIGNATURE_ALGORITHM}" \
  -validity "${VALIDITY_DAYS}" \
  -storetype "${STORE_TYPE}" \
  -keystore "${UNTRUSTED_CLIENT_KEYSTORE}" \
  -storepass "${STORE_PASSWORD}" \
  -keypass "${KEY_PASSWORD}" \
  -noprompt

echo "Generating unrelated server certificate..."

keytool -genkeypair \
  -alias "${UNRELATED_SERVER_ALIAS}" \
  -dname "${UNRELATED_SERVER_SUBJECT}" \
  -ext "SAN=dns:unrelated.invalid" \
  -ext "EKU=serverAuth" \
  -keyalg "${KEY_ALGORITHM}" \
  -keysize "${KEY_SIZE}" \
  -sigalg "${SIGNATURE_ALGORITHM}" \
  -validity "${VALIDITY_DAYS}" \
  -storetype "${STORE_TYPE}" \
  -keystore "${TEMP_DIR}/unrelated-server-keystore.p12" \
  -storepass "${STORE_PASSWORD}" \
  -keypass "${KEY_PASSWORD}" \
  -noprompt

echo "Exporting certificates..."

keytool -exportcert \
  -alias "${SERVER_ALIAS}" \
  -keystore "${SERVER_KEYSTORE}" \
  -storetype "${STORE_TYPE}" \
  -storepass "${STORE_PASSWORD}" \
  -file "${TEMP_DIR}/server.crt" \
  -rfc

keytool -exportcert \
  -alias "${TRUSTED_CLIENT_ALIAS}" \
  -keystore "${TRUSTED_CLIENT_KEYSTORE}" \
  -storetype "${STORE_TYPE}" \
  -storepass "${STORE_PASSWORD}" \
  -file "${TEMP_DIR}/trusted-client.crt" \
  -rfc

keytool -exportcert \
  -alias "${UNRELATED_SERVER_ALIAS}" \
  -keystore "${TEMP_DIR}/unrelated-server-keystore.p12" \
  -storetype "${STORE_TYPE}" \
  -storepass "${STORE_PASSWORD}" \
  -file "${TEMP_DIR}/unrelated-server.crt" \
  -rfc

echo "Creating server truststore..."

keytool -importcert \
  -alias "${TRUSTED_CLIENT_ALIAS}" \
  -file "${TEMP_DIR}/trusted-client.crt" \
  -keystore "${SERVER_TRUSTSTORE}" \
  -storetype "${STORE_TYPE}" \
  -storepass "${STORE_PASSWORD}" \
  -noprompt

echo "Creating client truststore..."

keytool -importcert \
  -alias "${SERVER_ALIAS}" \
  -file "${TEMP_DIR}/server.crt" \
  -keystore "${CLIENT_TRUSTSTORE}" \
  -storetype "${STORE_TYPE}" \
  -storepass "${STORE_PASSWORD}" \
  -noprompt

echo "Creating untrusted server truststore..."

keytool -importcert \
  -alias "${UNRELATED_SERVER_ALIAS}" \
  -file "${TEMP_DIR}/unrelated-server.crt" \
  -keystore "${UNTRUSTED_SERVER_TRUSTSTORE}" \
  -storetype "${STORE_TYPE}" \
  -storepass "${STORE_PASSWORD}" \
  -noprompt

echo
echo "TLS integration-test fixtures regenerated successfully."
echo
echo "Generated files:"
printf '  %s\n' \
  "${SERVER_KEYSTORE}" \
  "${SERVER_TRUSTSTORE}" \
  "${CLIENT_TRUSTSTORE}" \
  "${TRUSTED_CLIENT_KEYSTORE}" \
  "${UNTRUSTED_CLIENT_KEYSTORE}" \
  "${UNTRUSTED_SERVER_TRUSTSTORE}"

echo
echo "All store and private-key passwords: ${STORE_PASSWORD}"
echo "These fixtures are for testing only and must not be used in production."