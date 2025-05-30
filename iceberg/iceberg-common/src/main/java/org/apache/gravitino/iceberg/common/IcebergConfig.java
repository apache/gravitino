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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.OverwriteDefaultConfig;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

public class IcebergConfig extends Config implements OverwriteDefaultConfig {

  public static final String ICEBERG_CONFIG_PREFIX = "gravitino.iceberg-rest.";
  @VisibleForTesting public static final String ICEBERG_EXTENSION_PACKAGES = "extension-packages";

  public static final int DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT = 9001;
  public static final int DEFAULT_ICEBERG_REST_SERVICE_HTTPS_PORT = 9433;

  public static final ConfigEntry<String> CATALOG_BACKEND =
      new ConfigBuilder(IcebergConstants.CATALOG_BACKEND)
          .doc("Catalog backend of Gravitino Iceberg catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .createWithDefault("memory");

  public static final ConfigEntry<String> CATALOG_BACKEND_IMPL =
      new ConfigBuilder(IcebergConstants.CATALOG_BACKEND_IMPL)
          .doc(
              "The fully-qualified class name of a custom catalog implementation, "
                  + "only worked if `catalog-backend` is `custom`")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> CATALOG_WAREHOUSE =
      new ConfigBuilder(IcebergConstants.WAREHOUSE)
          .doc("Warehouse directory of catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
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
      new ConfigBuilder(S3Properties.GRAVITINO_S3_ENDPOINT)
          .doc(
              "An alternative endpoint of the S3 service, This could be used to for S3FileIO with "
                  + "any s3-compatible object storage service that has a different endpoint, or "
                  + "access a private S3 endpoint in a virtual private cloud")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> S3_REGION =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_REGION)
          .doc("The region of the S3 service")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> S3_ACCESS_KEY_ID =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID)
          .doc("The static access key ID used to access S3 data")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> S3_SECRET_ACCESS_KEY =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY)
          .doc("The static secret access key used to access S3 data")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public static final ConfigEntry<Boolean> S3_PATH_STYLE_ACCESS =
      new ConfigBuilder(S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS)
          .doc("Whether to use path style access for S3")
          .version(ConfigConstants.VERSION_0_9_0)
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<String> OSS_ENDPOINT =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ENDPOINT)
          .doc("The endpoint of Aliyun OSS service")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> OSS_ACCESS_KEY_ID =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID)
          .doc("The static access key ID used to access OSS data")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> OSS_ACCESS_KEY_SECRET =
      new ConfigBuilder(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET)
          .doc("The static access key secret used to access OSS data")
          .version(ConfigConstants.VERSION_0_7_0)
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
              "The retain days of Iceberg metrics, the value not greater than 0 means "
                  + "retain forever")
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

  public static final ConfigEntry<Long> ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL =
      new ConfigBuilder(IcebergConstants.ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL)
          .doc("Catalog cache eviction interval.")
          .version(ConfigConstants.VERSION_0_7_0)
          .longConf()
          .createWithDefault(3600000L);

  public static final ConfigEntry<String> ICEBERG_REST_CATALOG_CONFIG_PROVIDER =
      new ConfigBuilder(IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER)
          .doc(
              "Catalog provider class name, you can develop a class that implements "
                  + "`IcebergConfigProvider` and add the corresponding jar file to the Iceberg "
                  + "REST service classpath directory.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .createWithDefault(IcebergConstants.STATIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);

  public static final ConfigEntry<String> GRAVITINO_URI =
      new ConfigBuilder(IcebergConstants.GRAVITINO_URI)
          .doc(
              "The uri of Gravitino server address, only worked if `catalog-provider` is "
                  + "`gravitino-based-provider`.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> GRAVITINO_METALAKE =
      new ConfigBuilder(IcebergConstants.GRAVITINO_METALAKE)
          .doc(
              "The metalake name that `gravitino-based-provider` used to request to Gravitino, "
                  + "only worked if `catalog-provider` is `gravitino-based-provider`.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public static final ConfigEntry<List<String>> REST_API_EXTENSION_PACKAGES =
      new ConfigBuilder(ICEBERG_EXTENSION_PACKAGES)
          .doc("Comma-separated list of Iceberg REST API packages to expand")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .toSequence()
          .createWithDefault(Collections.emptyList());

  /**
   * Configuration entry for the single credential-provider type for Iceberg.
   *
   * @deprecated use {@link CredentialConstants#CREDENTIAL_PROVIDERS} instead
   */
  @Deprecated
  public static final ConfigEntry<String> CREDENTIAL_PROVIDER_TYPE =
      new ConfigBuilder(CredentialConstants.CREDENTIAL_PROVIDER_TYPE)
          .doc(
              String.format(
                  "Deprecated, please use %s instead, The credential provider type for Iceberg",
                  CredentialConstants.CREDENTIAL_PROVIDERS))
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .create();

  public String getJdbcDriver() {
    return get(JDBC_DRIVER);
  }

  public String getCatalogBackendName() {
    return IcebergPropertiesUtils.getCatalogBackendName(getAllConfig());
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
        ConfigConstants.WEBSERVER_HTTP_PORT,
        String.valueOf(DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT),
        ConfigConstants.WEBSERVER_HTTPS_PORT,
        String.valueOf(DEFAULT_ICEBERG_REST_SERVICE_HTTPS_PORT));
  }
}
