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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers {@code lakehouse-iceberg} Gravitino catalogs as native Iceberg REST Spark catalogs when
 * {@code spark.sql.gravitino.iceberg.enableRestAccess=true}.
 *
 * <p>For each catalog this sets:
 *
 * <pre>{@code
 * spark.sql.catalog.<name>           = org.apache.iceberg.spark.SparkCatalog
 * spark.sql.catalog.<name>.type      = rest
 * spark.sql.catalog.<name>.uri       = <resolvedRestUri>
 * spark.sql.catalog.<name>.warehouse = <name>
 * }</pre>
 *
 * When the catalog has {@code data-access=vended-credentials}, the delegation header is also set.
 * Otherwise, storage credentials and static storage properties are translated and set.
 */
public class IcebergRestCatalogRegistrar {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRestCatalogRegistrar.class);

  private static final String ICEBERG_SPARK_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog";
  private static final String CATALOG_TYPE_REST = "rest";
  /** Default HTTP port for the Gravitino Iceberg REST service (mirrors {@code IcebergConfig}). */
  private static final int ICEBERG_REST_DEFAULT_HTTP_PORT = 9001;
  /** Default HTTPS port for the Gravitino Iceberg REST service (mirrors {@code IcebergConfig}). */
  private static final int ICEBERG_REST_DEFAULT_HTTPS_PORT = 9433;

  private static final String ICEBERG_REST_DEFAULT_PATH = "/iceberg/";
  private static final String VENDED_CREDENTIALS = "vended-credentials";
  private static final String ACCESS_DELEGATION_HEADER = "header.X-Iceberg-Access-Delegation";

  /**
   * Storage-level Iceberg config keys that should be forwarded to the Spark REST catalog. These are
   * produced by {@link IcebergPropertiesUtils#toIcebergCatalogProperties} and are safe to pass
   * through; credential keys (access-key-id, secret-access-key, etc.) are handled separately via
   * {@link CredentialPropertyUtils}.
   */
  private static final Set<String> STORAGE_PROPERTY_KEYS =
      com.google.common.collect.ImmutableSet.of(
          IcebergConstants.ICEBERG_S3_ENDPOINT,
          IcebergConstants.AWS_S3_REGION,
          IcebergConstants.ICEBERG_S3_PATH_STYLE_ACCESS,
          IcebergConstants.ICEBERG_OSS_ENDPOINT,
          IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_NAME,
          IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_KEY);

  private final String resolvedRestUri;

  /**
   * Creates a registrar that resolves the Iceberg REST URI from the given Spark configuration.
   *
   * @param sparkConf the Spark configuration
   */
  public IcebergRestCatalogRegistrar(SparkConf sparkConf) {
    this.resolvedRestUri = resolveRestUri(sparkConf);
  }

  /**
   * Registers a single {@code lakehouse-iceberg} Gravitino catalog as a native Iceberg REST catalog
   * by injecting the required Spark catalog properties into {@code sparkConf}.
   *
   * @param sparkConf the Spark configuration to update
   * @param catalogName the Gravitino catalog name (becomes the Spark catalog name)
   * @param gravitinoCatalog the Gravitino catalog metadata object
   */
  public void registerCatalog(SparkConf sparkConf, String catalogName, Catalog gravitinoCatalog) {
    String sparkCatalogKey = "spark.sql.catalog." + catalogName;
    Preconditions.checkArgument(
        !sparkConf.contains(sparkCatalogKey),
        catalogName + " is already registered to SparkCatalogManager");

    sparkConf.set(sparkCatalogKey, ICEBERG_SPARK_CATALOG_CLASS);
    sparkConf.set(sparkCatalogKey + ".type", CATALOG_TYPE_REST);
    sparkConf.set(sparkCatalogKey + ".uri", resolvedRestUri);
    sparkConf.set(sparkCatalogKey + ".warehouse", catalogName);

    Map<String, String> catalogProperties = gravitinoCatalog.properties();
    String dataAccess = catalogProperties.get(IcebergConstants.DATA_ACCESS);

    if (VENDED_CREDENTIALS.equalsIgnoreCase(dataAccess)) {
      sparkConf.set(sparkCatalogKey + "." + ACCESS_DELEGATION_HEADER, VENDED_CREDENTIALS);
      LOG.info(
          "Registered Iceberg REST catalog {} with credential vending (warehouse={}).",
          catalogName,
          catalogName);
    } else {
      applyStorageConfig(sparkConf, sparkCatalogKey, gravitinoCatalog);
      LOG.info(
          "Registered Iceberg REST catalog {} with static storage config (warehouse={}).",
          catalogName,
          catalogName);
    }
  }

  /** Returns the resolved Iceberg REST URI used for all catalog registrations. */
  public String getResolvedRestUri() {
    return resolvedRestUri;
  }

  private void applyStorageConfig(
      SparkConf sparkConf, String sparkCatalogKey, Catalog gravitinoCatalog) {
    // Translate dynamic credentials (S3, OSS, ADLS tokens / keys) via CredentialPropertyUtils.
    try {
      Credential[] credentials = CredentialPropertyUtils.getCredentials(gravitinoCatalog);
      if (credentials != null && credentials.length > 0) {
        Map<String, String> credProps = new HashMap<>();
        CredentialPropertyUtils.applyIcebergCredentials(credentials, credProps);
        credProps.forEach((k, v) -> sparkConf.set(sparkCatalogKey + "." + k, v));
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to retrieve credentials for catalog from key prefix {}; proceeding without"
              + " dynamic credentials.",
          sparkCatalogKey,
          e);
    }

    // Translate static storage properties (endpoint, region, path-style, etc.).
    Map<String, String> icebergProps =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoCatalog.properties());
    icebergProps.forEach(
        (k, v) -> {
          if (STORAGE_PROPERTY_KEYS.contains(k)) {
            sparkConf.set(sparkCatalogKey + "." + k, v);
          }
        });
  }

  private static String resolveRestUri(SparkConf sparkConf) {
    String explicitUri = sparkConf.get(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI, null);
    if (StringUtils.isNotBlank(explicitUri)) {
      return explicitUri;
    }

    String gravitinoUri = sparkConf.get(GravitinoSparkConfig.GRAVITINO_URI);
    try {
      URI base = new URI(gravitinoUri);
      String scheme = base.getScheme();
      int defaultPort =
          "https".equalsIgnoreCase(scheme)
              ? ICEBERG_REST_DEFAULT_HTTPS_PORT
              : ICEBERG_REST_DEFAULT_HTTP_PORT;
      String inferred =
          scheme + "://" + base.getHost() + ":" + defaultPort + ICEBERG_REST_DEFAULT_PATH;
      LOG.warn(
          "spark.sql.gravitino.iceberg.restUri is not set; inferred Iceberg REST URI {} from"
              + " spark.sql.gravitino.uri={}. Production deployments should set the explicit"
              + " restUri override.",
          inferred,
          gravitinoUri);
      return inferred;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Cannot infer Iceberg REST URI from spark.sql.gravitino.uri=" + gravitinoUri, e);
    }
  }
}
