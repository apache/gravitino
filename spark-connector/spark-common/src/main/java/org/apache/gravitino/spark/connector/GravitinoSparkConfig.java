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

package org.apache.gravitino.spark.connector;

import org.apache.gravitino.auth.AuthProperties;

public class GravitinoSparkConfig {

  private static final String GRAVITINO_PREFIX = "spark.sql.gravitino.";
  public static final String GRAVITINO_URI = GRAVITINO_PREFIX + "uri";
  public static final String GRAVITINO_METALAKE = GRAVITINO_PREFIX + "metalake";
  public static final String GRAVITINO_ENABLE_ICEBERG_SUPPORT =
      GRAVITINO_PREFIX + "enableIcebergSupport";
  public static final String GRAVITINO_ENABLE_PAIMON_SUPPORT =
      GRAVITINO_PREFIX + "enablePaimonSupport";
  public static final String GRAVITINO_CLIENT_CONFIG_PREFIX = GRAVITINO_PREFIX + "client.";

  // Manually overrides the Gravitino Iceberg REST server endpoint that hive/jdbc backed
  // lakehouse-iceberg catalogs are routed through; takes precedence over auto-discovery.
  public static final String GRAVITINO_ICEBERG_REST_URI = GRAVITINO_PREFIX + "iceberg.rest-uri";

  // Reuses the Gravitino OAuth2 client configuration for automatically routed Iceberg REST
  // catalogs. Iceberg obtains and refreshes its own access token with the same client identity.
  public static final String GRAVITINO_ICEBERG_REUSE_OAUTH2 =
      GRAVITINO_PREFIX + "iceberg.reuseOAuth2";

  // Pass-through prefix for the Iceberg REST client config (e.g. rest.auth.type,
  // rest.auth.basic.username), applied when a catalog is routed through the Iceberg REST server.
  public static final String GRAVITINO_ICEBERG_REST_CONFIG_PREFIX =
      GRAVITINO_PREFIX + "iceberg.rest.";

  public static final String GRAVITINO_AUTH_TYPE =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_CLIENT_AUTH_TYPE;
  public static final String GRAVITINO_OAUTH2_URI =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_OAUTH2_SERVER_URI;
  public static final String GRAVITINO_OAUTH2_PATH =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_OAUTH2_TOKEN_PATH;
  public static final String GRAVITINO_OAUTH2_CREDENTIAL =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_OAUTH2_CREDENTIAL;
  public static final String GRAVITINO_OAUTH2_SCOPE =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_OAUTH2_SCOPE;
  public static final String GRAVITINO_BASIC_USERNAME =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_BASIC_USERNAME;
  public static final String GRAVITINO_BASIC_PASSWORD =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_BASIC_PASSWORD;
  public static final String GRAVITINO_KERBEROS_PRINCIPAL = "spark.kerberos.principal";
  public static final String GRAVITINO_KERBEROS_KEYTAB_FILE_PATH = "spark.kerberos.keytab";

  public static final String GRAVITINO_TOKEN_VALUE =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_TOKEN_VALUE;
  public static final String GRAVITINO_TOKEN_FILE =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_TOKEN_FILE;
  public static final String GRAVITINO_TOKEN_PRINCIPAL_FIELDS =
      GRAVITINO_PREFIX + AuthProperties.GRAVITINO_TOKEN_PRINCIPAL_FIELDS;
  public static final String GRAVITINO_TOKEN_PRINCIPAL_FIELDS_DEFAULT = "sub";

  public static final String GRAVITINO_CLIENT_CACHE_MAX_SIZE =
      GRAVITINO_PREFIX + "clientCacheMaxSize";
  public static final int GRAVITINO_CLIENT_CACHE_MAX_SIZE_DEFAULT = 100;
  public static final String GRAVITINO_CLIENT_CACHE_TTL_SEC =
      GRAVITINO_PREFIX + "clientCacheTtlSec";
  public static final long GRAVITINO_CLIENT_CACHE_TTL_SEC_DEFAULT = 3600;
  public static final String GRAVITINO_CATALOG_CACHE_TTL_SEC =
      GRAVITINO_PREFIX + "catalogCacheTtlSec";
  public static final long GRAVITINO_CATALOG_CACHE_TTL_SEC_DEFAULT = 300;

  public static final String GRAVITINO_HIVE_METASTORE_URI = "metastore.uris";
  public static final String SPARK_HIVE_METASTORE_URI = "hive.metastore.uris";

  private GravitinoSparkConfig() {}
}
