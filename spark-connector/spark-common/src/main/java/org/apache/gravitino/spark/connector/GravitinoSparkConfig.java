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

  /**
   * When true, registers {@code lakehouse-iceberg} Gravitino catalogs as native Iceberg REST
   * catalogs ({@code org.apache.iceberg.spark.SparkCatalog} with {@code type=rest}) instead of the
   * Gravitino wrapper catalog. Mutually exclusive with {@link #GRAVITINO_ENABLE_ICEBERG_SUPPORT}
   * for a given catalog.
   */
  public static final String GRAVITINO_ICEBERG_ENABLE_REST_ACCESS =
      GRAVITINO_PREFIX + "iceberg.enableRestAccess";

  /**
   * Explicit URI for the Gravitino Iceberg REST service, e.g. {@code http://host:9001/iceberg/}.
   * When omitted, the URI is inferred from {@link #GRAVITINO_URI} by substituting the default
   * Iceberg REST port (9001) and path ({@code /iceberg/}).
   */
  public static final String GRAVITINO_ICEBERG_REST_URI = GRAVITINO_PREFIX + "iceberg.restUri";

  public static final String GRAVITINO_CLIENT_CONFIG_PREFIX = GRAVITINO_PREFIX + "client.";

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

  public static final String GRAVITINO_HIVE_METASTORE_URI = "metastore.uris";
  public static final String SPARK_HIVE_METASTORE_URI = "hive.metastore.uris";

  private GravitinoSparkConfig() {}
}
