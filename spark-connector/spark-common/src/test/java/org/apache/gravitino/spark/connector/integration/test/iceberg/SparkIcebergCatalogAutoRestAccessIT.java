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
package org.apache.gravitino.spark.connector.integration.test.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * Integration tests for automatic Iceberg REST catalog registration via {@code
 * spark.sql.gravitino.iceberg.enableRestAccess=true}.
 *
 * <p>Setup:
 *
 * <ul>
 *   <li>The Gravitino catalog is a standard {@code lakehouse-iceberg} catalog backed by Hive.
 *   <li>The Gravitino auxiliary Iceberg REST service is started with {@code
 *       dynamic-config-provider}, so it loads catalog configuration from Gravitino at request time
 *       and exposes the catalog under its Gravitino name.
 *   <li>Spark is configured with {@code spark.sql.gravitino.iceberg.enableRestAccess=true} (and
 *       {@code enableIcebergSupport} disabled), so the Gravitino plugin registers a native {@code
 *       org.apache.iceberg.spark.SparkCatalog} with {@code type=rest} for each Iceberg catalog.
 * </ul>
 *
 * <p>Most Iceberg DDL/DML tests inherited from {@link SparkIcebergCatalogIT} are expected to pass
 * because the native Iceberg REST catalog supports the same table operations. Function tests are
 * disabled because functions are managed through the Gravitino catalog wrapper, not the REST
 * catalog.
 */
@Tag("gravitino-docker-test")
// The native Iceberg REST catalog client shipped with the Spark connector runtime uses a lower
// Iceberg version than the Gravitino REST server, which can cause incompatibilities in embedded
// mode.  The non-embedded (standalone Gravitino + Docker Hive) path works correctly.
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public abstract class SparkIcebergCatalogAutoRestAccessIT extends SparkIcebergCatalogIT {

  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";

  @Override
  protected boolean supportsFunction() {
    // Functions are managed via the Gravitino catalog wrapper; they are not accessible through
    // the native Iceberg REST catalog registered by enableRestAccess.
    return false;
  }

  /**
   * Returns catalog properties for a standard Hive-backed Iceberg catalog. The Gravitino auxiliary
   * Iceberg REST service (configured with dynamic-config-provider) will pick up this configuration
   * automatically and expose it under the Gravitino catalog name.
   */
  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> props = Maps.newHashMap();
    props.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    props.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, hiveMetastoreUri);
    props.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    return props;
  }

  /**
   * Overrides the default static-backend Iceberg REST service configuration to use {@code
   * dynamic-config-provider}. With this provider the REST server authenticates to Gravitino and
   * loads catalog configuration on-demand, routing requests to the correct catalog via the {@code
   * warehouse} parameter.
   */
  @Override
  protected void initIcebergRestServiceEnv() {
    super.ignoreIcebergAuxRestService = false;
    registerCustomConfigs(
        ImmutableMap.of(
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
            IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE,
            "test",
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_SIMPLE_USERNAME,
            "anonymous"));
  }

  /**
   * Disables the default {@code enableIcebergSupport} flag and enables {@code enableRestAccess} so
   * the Gravitino plugin registers native Iceberg REST catalogs.
   */
  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    return ImmutableMap.of(
        GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, "false",
        GravitinoSparkConfig.GRAVITINO_ICEBERG_ENABLE_REST_ACCESS, "true");
  }
}
