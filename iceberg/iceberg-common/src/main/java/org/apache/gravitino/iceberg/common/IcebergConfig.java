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

package org.apache.gravitino.iceberg.common;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.OverwriteDefaultConfig;

public class IcebergConfig extends Config implements OverwriteDefaultConfig {

  public static final String ICEBERG_CONFIG_PREFIX = "gravitino.iceberg-rest.";

  public static final ConfigEntry<String> CATALOG_BACKEND =
      new ConfigBuilder(IcebergConstants.CATALOG_BACKEND)
          .doc("Catalog backend of Gravitino Iceberg catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .createWithDefault("memory");

  public static final ConfigEntry<String> CATALOG_WAREHOUSE =
      new ConfigBuilder(IcebergConstants.WAREHOUSE)
          .doc("Warehouse directory of catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> CATALOG_URI =
      new ConfigBuilder(IcebergConstants.URI)
          .doc("The uri config of the Iceberg catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_USER =
      new ConfigBuilder(IcebergConstants.GRAVITINO_JDBC_USER)
          .doc("The username of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_PASSWORD =
      new ConfigBuilder(IcebergConstants.GRAVITINO_JDBC_PASSWORD)
          .doc("The password of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();
  public static final ConfigEntry<String> JDBC_DRIVER =
      new ConfigBuilder(IcebergConstants.GRAVITINO_JDBC_DRIVER)
          .doc("The driver of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<Boolean> JDBC_INIT_TABLES =
      new ConfigBuilder(IcebergConstants.ICEBERG_JDBC_INITIALIZE)
          .doc("Whether to initialize meta tables when create Jdbc catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .booleanConf()
          .createWithDefault(true);

  public static final ConfigEntry<String> IO_IMPL =
      new ConfigBuilder(IcebergConstants.IO_IMPL)
          .doc("The io implementation for `FileIO` in Iceberg")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> S3_ENDPOINT =
      new ConfigBuilder(IcebergConstants.GRAVITINO_S3_ENDPOINT)
          .doc(
              "An alternative endpoint of the S3 service, This could be used to for S3FileIO with "
                  + "any s3-compatible object storage service that has a different endpoint, or "
                  + "access a private S3 endpoint in a virtual private cloud")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> S3_REGION =
      new ConfigBuilder(IcebergConstants.GRAVITINO_S3_REGION)
          .doc("The region of the S3 service")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> S3_ACCESS_KEY_ID =
      new ConfigBuilder(IcebergConstants.GRAVITINO_S3_ACCESS_KEY_ID)
          .doc("The static access key ID used to access S3 data")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> S3_SECRET_ACCESS_KEY =
      new ConfigBuilder(IcebergConstants.GRAVITINO_S3_SECRET_ACCESS_KEY)
          .doc("The static secret access key used to access S3 data")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> ICEBERG_METRICS_STORE =
      new ConfigBuilder(IcebergConstants.ICEBERG_METRICS_STORE)
          .doc("The store to save Iceberg metrics")
          .version(ConfigConstants.VERSION_0_4_0)
          .stringConf()
          .create();

  public static final ConfigEntry<Integer> ICEBERG_METRICS_STORE_RETAIN_DAYS =
      new ConfigBuilder(IcebergConstants.ICEBERG_METRICS_STORE_RETAIN_DAYS)
          .doc(
              "The retain days of Iceberg metrics, the value not greater than 0 means retain forever")
          .version(ConfigConstants.VERSION_0_4_0)
          .intConf()
          .createWithDefault(-1);

  public static final ConfigEntry<Integer> ICEBERG_METRICS_QUEUE_CAPACITY =
      new ConfigBuilder(IcebergConstants.ICEBERG_METRICS_QUEUE_CAPACITY)
          .doc("The capacity for Iceberg metrics queues, should greater than 0")
          .version(ConfigConstants.VERSION_0_4_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(1000);

  public static final ConfigEntry<String> CATALOG_BACKEND_NAME =
      new ConfigBuilder(IcebergConstants.CATALOG_BACKEND_NAME)
          .doc("The catalog name for Iceberg catalog backend")
          .version(ConfigConstants.VERSION_0_5_2)
          .stringConf()
          .create();

  public String getJdbcDriver() {
    return get(JDBC_DRIVER);
  }

  public String getCatalogBackendName(String defaultCatalogBackendName) {
    return Optional.ofNullable(get(CATALOG_BACKEND_NAME)).orElse(defaultCatalogBackendName);
  }

  public IcebergConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public IcebergConfig() {
    super(false);
  }

  public Map<String, String> getIcebergCatalogProperties() {
    Map<String, String> config = getAllConfig();
    Map<String, String> transformedConfig =
        IcebergPropertiesUtils.toIcebergCatalogProperties(config);
    transformedConfig.putAll(config);
    return transformedConfig;
  }

  @Override
  public Map<String, String> getOverwriteDefaultConfig() {
    return ImmutableMap.of(
        JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT),
        JettyServerConfig.WEBSERVER_HTTPS_PORT.getKey(),
        String.valueOf(JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTPS_PORT));
  }
}
