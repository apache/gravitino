/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.lakehouse.iceberg;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class IcebergConstants {

  // Iceberg catalog properties constants
  public static final String CATALOG_BACKEND = "catalog-backend";
  public static final String CATALOG_BACKEND_IMPL = "catalog-backend-impl";

  /** Internal property containing the unique identifier of a Gravitino catalog. */
  public static final String CATALOG_UUID = "catalog_uuid";

  public static final String GRAVITINO_JDBC_USER = "jdbc-user";
  public static final String ICEBERG_JDBC_USER = "jdbc.user";

  public static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
  public static final String ICEBERG_JDBC_PASSWORD = "jdbc.password";
  public static final String ICEBERG_JDBC_INITIALIZE = "jdbc-initialize";

  public static final String DATA_ACCESS = "data-access";

  public static final String ICEBERG_ACCESS_DELEGATION = "header.X-Iceberg-Access-Delegation";

  public static final String GRAVITINO_JDBC_SCHEMA_VERSION = "jdbc-schema-version";
  public static final String ICEBERG_JDBC_SCHEMA_VERSION = "jdbc.schema-version";

  public static final String ICEBERG_JDBC_STRICT_MODE = "jdbc.strict-mode";

  public static final String GRAVITINO_JDBC_DRIVER = "jdbc-driver";
  public static final String WAREHOUSE = "warehouse";
  public static final String URI = "uri";
  public static final String CATALOG_BACKEND_NAME = "catalog-backend-name";

  /** Catalog property: the format version of a new table that does not request one. */
  public static final String TABLE_FORMAT_VERSION_DEFAULT = "table-format-version.default";

  /** Catalog property: the highest format version a table may be created at or upgraded to. */
  public static final String TABLE_FORMAT_VERSION_MAX = "table-format-version.max";

  /** Iceberg catalog property that sets the format version of tables created without one. */
  public static final String ICEBERG_TABLE_DEFAULT_FORMAT_VERSION = "table-default.format-version";

  /**
   * The format version of a new table when neither the request nor {@link
   * #TABLE_FORMAT_VERSION_DEFAULT} names one.
   */
  public static final int DEFAULT_TABLE_FORMAT_VERSION = 2;

  /**
   * The Iceberg table format versions Gravitino accepts: {@code 1} to {@code 4}, the range the
   * bundled Iceberg version (1.11) can write.
   */
  public static final Set<Integer> SUPPORTED_TABLE_FORMAT_VERSIONS =
      Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4)));

  /**
   * The highest format version this Gravitino build accepts, and the one a table may be created at
   * or upgraded to when {@link #TABLE_FORMAT_VERSION_MAX} is unset. {@link
   * #TABLE_FORMAT_VERSION_MAX} can only lower it. It tracks the bundled Iceberg's highest writable
   * version, and a test fails when the two differ.
   */
  public static final int DEFAULT_MAX_TABLE_FORMAT_VERSION =
      Collections.max(SUPPORTED_TABLE_FORMAT_VERSIONS);

  // IO properties
  public static final String IO_IMPL = "io-impl";
  public static final String ICEBERG_S3_ENDPOINT = "s3.endpoint";
  public static final String ICEBERG_S3_ACCESS_KEY_ID = "s3.access-key-id";
  public static final String ICEBERG_S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  public static final String ICEBERG_S3_TOKEN = "s3.session-token";
  public static final String ICEBERG_S3_PATH_STYLE_ACCESS = "s3.path-style-access";
  public static final String AWS_S3_REGION = "client.region";

  public static final String ICEBERG_OSS_ENDPOINT = "oss.endpoint";
  public static final String ICEBERG_OSS_ACCESS_KEY_ID = "client.access-key-id";
  public static final String ICEBERG_OSS_ACCESS_KEY_SECRET = "client.access-key-secret";

  public static final String ICEBERG_ADLS_STORAGE_ACCOUNT_NAME =
      "adls.auth.shared-key.account.name";
  public static final String ICEBERG_ADLS_STORAGE_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

  /** Iceberg property that specifies the ADLS token credential provider implementation. */
  public static final String ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER =
      "adls.token-credential-provider";

  /** Prefix for properties passed to the Iceberg ADLS token credential provider. */
  public static final String ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER_PREFIX =
      ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER + ".";

  /** Gravitino's Azure client-secret token credential provider implementation. */
  public static final String AZURE_CLIENT_SECRET_TOKEN_CREDENTIAL_PROVIDER =
      "org.apache.gravitino.iceberg.common.credential.AzureClientSecretTokenCredentialProvider";

  /** Iceberg GCSFileIO OAuth2 access token property. */
  public static final String ICEBERG_GCS_OAUTH2_TOKEN = "gcs.oauth2.token";

  /** Iceberg GCSFileIO OAuth2 token expiry property (epoch millis). */
  public static final String ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";

  /**
   * Whether Iceberg GCSFileIO should refresh OAuth2 tokens via a credentials endpoint. Defaults to
   * true in Iceberg; Gravitino disables it when minting a token from {@code
   * gcs-service-account-file} because that path has no table credentials refresh endpoint.
   */
  public static final String ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED =
      "gcs.oauth2.refresh-credentials-enabled";

  // Iceberg Table properties constants

  public static final String COMMENT = "comment";
  public static final String CREATOR = "creator";
  public static final String OWNER = "owner";
  public static final String LOCATION = "location";
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  public static final String CHERRY_PICK_SNAPSHOT_ID = "cherry-pick-snapshot-id";
  public static final String SORT_ORDER = "sort-order";
  public static final String IDENTIFIER_FIELDS = "identifier-fields";
  public static final String PROVIDER = "provider";
  public static final String FORMAT = "format";
  public static final String FORMAT_VERSION = "format-version";

  public static final String ICEBERG_METRICS_STORE = "metricsStore";
  public static final String ICEBERG_METRICS_STORE_RETAIN_DAYS = "metricsStoreRetainDays";
  public static final String ICEBERG_METRICS_QUEUE_CAPACITY = "metricsQueueCapacity";

  public static final String GRAVITINO_ICEBERG_REST_SERVICE_NAME = "iceberg-rest";

  public static final String ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL =
      "catalog-cache-eviction-interval-ms";

  public static final String ICEBERG_REST_CATALOG_CONFIG_PROVIDER = "catalog-config-provider";
  public static final String STATIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME = "static-config-provider";
  public static final String DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME =
      "dynamic-config-provider";

  private static final String GRAVITINO_PREFIX = "gravitino-";
  public static final String GRAVITINO_URI = GRAVITINO_PREFIX + "uri";
  public static final String GRAVITINO_METALAKE = GRAVITINO_PREFIX + "metalake";
  public static final String GRAVITINO_AUTH_TYPE = GRAVITINO_PREFIX + "auth-type";
  public static final String GRAVITINO_SIMPLE_USERNAME = GRAVITINO_PREFIX + "simple.user-name";
  public static final String GRAVITINO_OAUTH2_SERVER_URI = GRAVITINO_PREFIX + "oauth2.server-uri";
  public static final String GRAVITINO_OAUTH2_CREDENTIAL = GRAVITINO_PREFIX + "oauth2.credential";
  public static final String GRAVITINO_OAUTH2_TOKEN_PATH = GRAVITINO_PREFIX + "oauth2.token-path";
  public static final String GRAVITINO_OAUTH2_SCOPE = GRAVITINO_PREFIX + "oauth2.scope";

  public static final String ICEBERG_REST_DEFAULT_METALAKE = "gravitino";
  public static final String ICEBERG_REST_DEFAULT_CATALOG = "default_catalog";
  public static final String ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME = "default-catalog-name";
  public static final String ICEBERG_REST_DISABLE_REST_AUTHZ = "disable-rest-authz";
  /** Gravitino config key for REST catalog backend HTTP connection timeout. */
  public static final String REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS =
      "rest-client-connection-timeout-ms";

  /** Gravitino config key for REST catalog backend HTTP socket timeout. */
  public static final String REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS =
      "rest-client-socket-timeout-ms";

  /** Iceberg REST client property key for HTTP connection timeout. */
  public static final String ICEBERG_REST_CLIENT_CONNECTION_TIMEOUT_MS =
      "rest.client.connection-timeout-ms";

  /** Iceberg REST client property key for HTTP socket timeout. */
  public static final String ICEBERG_REST_CLIENT_SOCKET_TIMEOUT_MS =
      "rest.client.socket-timeout-ms";

  public static final String TABLE_METADATA_CACHE_IMPL = "table-metadata-cache-impl";
  public static final String TABLE_METADATA_CACHE_CAPACITY = "table-metadata-cache-capacity";
  public static final String TABLE_METADATA_CACHE_EXPIRE_MINUTES =
      "table-metadata-cache-expire-minutes";

  public static final String SCAN_PLAN_CACHE_IMPL = "scan-plan-cache-impl";
  public static final String SCAN_PLAN_CACHE_CAPACITY = "scan-plan-cache-capacity";
  public static final String SCAN_PLAN_CACHE_EXPIRE_MINUTES = "scan-plan-cache-expire-minutes";
}
