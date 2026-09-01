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

package org.apache.gravitino.spark.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transform Apache Iceberg catalog properties between Apache Spark and Apache Gravitino. */
public class IcebergPropertiesConverter implements PropertiesConverter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergPropertiesConverter.class);

  /** Routing keys the connector always derives itself; they cannot be overridden. */
  private static final List<String> RESERVED_REST_PROPERTIES =
      ImmutableList.of(
          IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
          IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
          IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
          IcebergPropertiesConstants.ICEBERG_REST_CATALOG_PREFIX);

  public static class IcebergPropertiesConverterHolder {
    private static final IcebergPropertiesConverter INSTANCE = new IcebergPropertiesConverter();
  }

  private IcebergPropertiesConverter() {}

  public static IcebergPropertiesConverter getInstance() {
    return IcebergPropertiesConverter.IcebergPropertiesConverterHolder.INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(
        properties != null, "Iceberg Catalog properties should not be null");
    Map<String, String> all = IcebergPropertiesUtils.toIcebergCatalogProperties(properties);
    String catalogBackend = all.remove(IcebergConstants.CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogBackend),
        String.format("%s should not be empty", IcebergConstants.CATALOG_BACKEND));
    if (catalogBackend.equalsIgnoreCase(IcebergCatalogBackend.CUSTOM.name())) {
      String catalogBackendImpl = all.remove(IcebergConstants.CATALOG_BACKEND_IMPL);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(catalogBackendImpl),
          String.format(
              "%s should not be empty when %s is %s",
              IcebergConstants.CATALOG_BACKEND_IMPL,
              IcebergConstants.CATALOG_BACKEND,
              IcebergCatalogBackend.CUSTOM.name()));
      all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL, catalogBackendImpl);
    } else {
      all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE, catalogBackend);
    }

    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED, "FALSE");
    return all;
  }

  /**
   * Builds Spark Iceberg catalog properties that route requests through the Gravitino Iceberg REST
   * server, regardless of the catalog's actual storage backend (hive/jdbc). This is the only path
   * on which temporary credentials work: the Iceberg REST protocol vends a fresh credential per
   * table access, so static secrets are intentionally not carried over from {@code
   * gravitinoProperties}. Static-key credentials for the non-REST path are unaffected; they are
   * still injected via {@link org.apache.gravitino.credential.CredentialPropertyUtils}.
   *
   * @param gravitinoCatalogName the Gravitino catalog name, used as both {@code warehouse} (for the
   *     initial catalog discovery request) and {@code prefix} (for every subsequent request path
   *     segment) so the REST server resolves the same Gravitino catalog
   * @param restUri the Iceberg REST server endpoint to route through, resolved by the caller
   * @param gravitinoProperties the Gravitino catalog properties
   * @param icebergRestClientConfig operator-level Iceberg REST client config (e.g. {@code
   *     rest.auth.type}), applied after catalog-level {@code spark.bypass.} overrides since it is a
   *     cluster-wide operational setting
   * @return the Spark Iceberg catalog properties
   */
  Map<String, String> buildIcebergRestProperties(
      String gravitinoCatalogName,
      String restUri,
      Map<String, String> gravitinoProperties,
      Map<String, String> icebergRestClientConfig) {
    Preconditions.checkArgument(StringUtils.isNotBlank(restUri), "restUri should not be blank");

    Map<String, String> all = new HashMap<>();
    // Later put/putAll calls override earlier ones for the same key; this is call-order
    // precedence, unrelated to HashMap's (unspecified) iteration order.
    all.putAll(buildStorageProperties(gravitinoProperties));
    all.put(
        IcebergPropertiesConstants.ICEBERG_ACCESS_DELEGATION,
        IcebergPropertiesConstants.ICEBERG_ACCESS_DELEGATION_VENDED_CREDENTIALS);
    // The catalog's own spark.bypass properties override the defaults above, so a renamed
    // Iceberg client property can be worked around without a connector change.
    all.putAll(extractSparkBypassProperties(gravitinoProperties));
    all.putAll(icebergRestClientConfig);

    reapplyReservedRestProperties(gravitinoCatalogName, restUri, all);
    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED, "FALSE");
    return all;
  }

  /**
   * Re-derives the reserved routing keys ({@code type}/{@code uri}/{@code warehouse}/{@code
   * prefix}) on {@code all}, so that a source merged in after {@link #buildIcebergRestProperties}
   * (e.g. Spark catalog {@code options}) can never redirect a routed catalog.
   *
   * @param gravitinoCatalogName the Gravitino catalog name
   * @param restUri the Iceberg REST server endpoint to route through
   * @param all the properties to re-derive the reserved keys on, mutated in place
   */
  void reapplyReservedRestProperties(
      String gravitinoCatalogName, String restUri, Map<String, String> all) {
    warnOnReservedRestPropertyOverrides(gravitinoCatalogName, all);
    all.put(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST);
    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_URI, restUri);
    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE, gravitinoCatalogName);
    all.put(IcebergPropertiesConstants.ICEBERG_REST_CATALOG_PREFIX, gravitinoCatalogName);
  }

  private Map<String, String> extractSparkBypassProperties(Map<String, String> properties) {
    Map<String, String> bypass = new HashMap<>();
    if (properties != null) {
      properties.forEach(
          (k, v) -> {
            if (k.startsWith(SPARK_PROPERTY_PREFIX)) {
              bypass.put(k.substring(SPARK_PROPERTY_PREFIX.length()), v);
            }
          });
    }
    return bypass;
  }

  private void warnOnReservedRestPropertyOverrides(
      String gravitinoCatalogName, Map<String, String> config) {
    for (String reserved : RESERVED_REST_PROPERTIES) {
      if (config.containsKey(reserved)) {
        LOG.warn(
            "Property '{}' set on catalog '{}' is ignored; the connector always derives it when "
                + "routing through the Iceberg REST server.",
            reserved,
            gravitinoCatalogName);
      }
    }
  }

  /**
   * Derives the storage-related Iceberg client config from the catalog's warehouse location. Only
   * non-secret settings (native FileIO impl, custom endpoint, region, path-style access) are
   * carried over; static access keys and JDBC credentials are deliberately excluded since the REST
   * path vends its own temporary credentials.
   */
  private Map<String, String> buildStorageProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> icebergProperties =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProperties);
    Map<String, String> storageProperties = new HashMap<>();
    copyIfPresent(icebergProperties, IcebergPropertiesConstants.ICEBERG_IO_IMPL, storageProperties);

    String warehouse = gravitinoProperties.get(IcebergConstants.WAREHOUSE);
    String fileIoImpl = deriveFileIoImpl(warehouse);
    if (fileIoImpl != null) {
      storageProperties.putIfAbsent(IcebergPropertiesConstants.ICEBERG_IO_IMPL, fileIoImpl);
    } else {
      warnOnSchemeWithoutNativeFileIo(gravitinoProperties, warehouse);
    }

    copyIfPresent(
        icebergProperties, IcebergPropertiesConstants.ICEBERG_S3_ENDPOINT, storageProperties);
    copyIfPresent(
        icebergProperties, IcebergPropertiesConstants.ICEBERG_AWS_S3_REGION, storageProperties);
    copyIfPresent(
        icebergProperties,
        IcebergPropertiesConstants.ICEBERG_S3_PATH_STYLE_ACCESS,
        storageProperties);
    copyIfPresent(
        icebergProperties, IcebergPropertiesConstants.ICEBERG_OSS_ENDPOINT, storageProperties);
    return storageProperties;
  }

  /**
   * Derives the native Iceberg FileIO implementation for a warehouse location, or {@code null} if
   * the scheme has none (e.g. {@code hdfs://}, {@code file://}, {@code oss://}). Package-private so
   * the routing decision can check whether a warehouse has a native FileIO without duplicating the
   * scheme list.
   */
  static String deriveFileIoImpl(String warehouse) {
    if (StringUtils.isBlank(warehouse) || !warehouse.contains("://")) {
      return null;
    }
    String scheme = StringUtils.substringBefore(warehouse, "://").toLowerCase(Locale.ROOT);
    switch (scheme) {
      case "s3":
      case "s3a":
      case "s3n":
        return IcebergPropertiesConstants.ICEBERG_S3_FILE_IO_IMPL;
      case "gs":
        return IcebergPropertiesConstants.ICEBERG_GCS_FILE_IO_IMPL;
      case "abfs":
      case "abfss":
      case "wasb":
      case "wasbs":
        return IcebergPropertiesConstants.ICEBERG_ADLS_FILE_IO_IMPL;
      default:
        return null;
    }
  }

  /**
   * Warns when a warehouse scheme has no native Iceberg FileIO but the catalog vends credentials.
   * Vended credentials are only consumed by Iceberg's native FileIO implementations, so table
   * access would otherwise fail at read time with a storage authentication error far from its
   * cause, unless {@code io-impl} is set explicitly on the catalog.
   */
  private void warnOnSchemeWithoutNativeFileIo(
      Map<String, String> gravitinoProperties, String warehouse) {
    if (StringUtils.isBlank(warehouse) || !warehouse.contains("://")) {
      // hdfs and file legitimately have no native FileIO here, and neither vends credentials.
      return;
    }
    if (StringUtils.isBlank(gravitinoProperties.get(CredentialConstants.CREDENTIAL_PROVIDERS))) {
      return;
    }
    LOG.warn(
        "Warehouse '{}' has no native Iceberg FileIO for the credentials vended by '{}' to be "
            + "applied to; table access may fail to authenticate unless 'io-impl' is set "
            + "explicitly. Schemes with a native FileIO: s3/s3a/s3n, gs, abfs/abfss/wasb/wasbs.",
        warehouse,
        gravitinoProperties.get(CredentialConstants.CREDENTIAL_PROVIDERS));
  }

  private void copyIfPresent(Map<String, String> source, String key, Map<String, String> target) {
    String value = source.get(key);
    if (StringUtils.isNotBlank(value)) {
      target.put(key, value);
    }
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
