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

package org.apache.gravitino.trino.connector.catalog.iceberg;

import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.storage.S3Properties;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogPropertyConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A property converter for Iceberg catalogs that handles the conversion between Trino and Gravitino
 * property formats. This converter manages various Iceberg-specific configurations including
 * general settings, Hive integration, and S3 storage options.
 */
public class IcebergCatalogPropertyConverter extends CatalogPropertyConverter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogPropertyConverter.class);

  private static final Set<String> JDBC_BACKEND_REQUIRED_PROPERTIES = Set.of("jdbc-driver", "uri");

  private static final Set<String> HIVE_BACKEND_REQUIRED_PROPERTIES = Set.of("uri");

  private static final Set<String> REST_BACKEND_REQUIRED_PROPERTIES = Set.of("uri");

  private static final String TRINO_ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
  private static final String TRINO_ICEBERG_CATALOG_TYPE_REST = "rest";
  private static final String TRINO_ICEBERG_REST_URI = "iceberg.rest-catalog.uri";
  private static final String TRINO_ICEBERG_REST_PREFIX = "iceberg.rest-catalog.prefix";
  private static final String TRINO_ICEBERG_REST_WAREHOUSE = "iceberg.rest-catalog.warehouse";
  private static final String TRINO_ICEBERG_REST_VENDED_CREDENTIALS =
      "iceberg.rest-catalog.vended-credentials-enabled";
  private static final String TRINO_ICEBERG_REST_SESSION = "iceberg.rest-catalog.session";
  private static final String TRINO_FS_HADOOP_ENABLED = "fs.hadoop.enabled";
  private static final String TRINO_FS_NATIVE_S3_ENABLED = "fs.native-s3.enabled";
  private static final String TRINO_FS_NATIVE_GCS_ENABLED = "fs.native-gcs.enabled";
  private static final String TRINO_FS_NATIVE_AZURE_ENABLED = "fs.native-azure.enabled";
  private static final String TRINO_S3_REGION = "s3.region";
  private static final String TRINO_S3_ENDPOINT = "s3.endpoint";
  private static final String TRINO_S3_PATH_STYLE_ACCESS = "s3.path-style-access";

  /** Routing keys the connector always derives itself; they cannot be overridden. */
  private static final List<String> RESERVED_REST_PROPERTIES =
      List.of(
          TRINO_ICEBERG_CATALOG_TYPE,
          TRINO_ICEBERG_REST_URI,
          TRINO_ICEBERG_REST_WAREHOUSE,
          TRINO_ICEBERG_REST_PREFIX);

  /**
   * Injects credentials from credential vending into the Iceberg catalog config. Applies JDBC
   * user/password for the JDBC backend and S3 credentials for S3-backed storage.
   *
   * @param credentials the credentials returned by the server
   * @param config the mutable Trino Iceberg connector config map to update
   */
  public static void applyCredentials(Credential[] credentials, Map<String, String> config) {
    for (Credential credential : credentials) {
      if (credential instanceof JdbcCredential) {
        JdbcCredential jdbc = (JdbcCredential) credential;
        config.put("iceberg.jdbc-catalog.connection-user", jdbc.jdbcUser());
        config.put("iceberg.jdbc-catalog.connection-password", jdbc.jdbcPassword());
      } else if (credential instanceof S3SecretKeyCredential) {
        S3SecretKeyCredential s3 = (S3SecretKeyCredential) credential;
        config.put("hive.s3.aws-access-key", s3.accessKeyId());
        config.put("hive.s3.aws-secret-key", s3.secretAccessKey());
      } else if (credential instanceof AzureAccountKeyCredential) {
        AzureAccountKeyCredential azure = (AzureAccountKeyCredential) credential;
        config.put(
            String.format("fs.azure.account.key.%s.dfs.core.windows.net", azure.accountName()),
            azure.accountKey());
      }
    }
  }

  @Override
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> properties) {
    Map<String, String> stringStringMap;
    String backend = properties.get("catalog-backend");
    if (backend == null)
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property 'catalog-backend'");
    // Gravitino accepts the backend as an enum value, and its own consumers upper-case it before
    // resolving, so the value reaching here may be in any case.
    switch (backend.toLowerCase(Locale.ROOT)) {
      case "hive":
        stringStringMap = buildHiveBackendProperties(properties);
        break;
      case "jdbc":
        stringStringMap = buildJDBCBackendProperties(properties);
        break;
      case "rest":
        stringStringMap = buildRestBackendProperties(properties);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported backend type: " + backend);
    }
    Map<String, String> config = new HashMap<>();
    // Later put/putAll calls override earlier ones for the same key; this is call-order
    // precedence, unrelated to HashMap's (unspecified) iteration order.
    config.putAll(super.gravitinoToEngineProperties(properties));
    config.putAll(stringStringMap);
    config.put("fs.hadoop.enabled", "true");
    return config;
  }

  /**
   * Builds the Trino Iceberg connector config that reaches this catalog through the Gravitino
   * Iceberg REST server, regardless of the catalog backend Gravitino uses to store its metadata.
   *
   * <p>This is the only path on which <em>temporary</em> credentials work — {@code s3-token} and
   * its GCS and ADLS counterparts. The Iceberg REST protocol issues a fresh one per table access,
   * while Trino's jdbc and hive_metastore Iceberg catalog types have nowhere to put the session
   * token of an STS credential. Static-key credentials are unaffected: they are still injected on
   * the backend path by {@link #applyCredentials}.
   *
   * @param catalog the Gravitino catalog to load
   * @param gravitinoConfig the connector configuration holding the Iceberg REST catalog's
   *     authentication and other pass-through settings
   * @param restUri the Iceberg REST server endpoint to route through, resolved by the caller
   * @return the Trino Iceberg connector config
   * @throws TrinoException if {@code restUri} is blank
   */
  public Map<String, String> buildIcebergRestProperties(
      GravitinoCatalog catalog, GravitinoConfig gravitinoConfig, String restUri) {
    if (StringUtils.isBlank(restUri)) {
      // The caller only reaches this method once it has already confirmed a non-blank URI, so
      // this is defensive: it can only fire if that URI disappeared between the two calls.
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
          "No Iceberg REST server endpoint is available for metalake '"
              + catalog.getMetalake()
              + "'. Set 'gravitino.iceberg.rest-uri' to override the address the Gravitino "
              + "server reports.");
    }

    Map<String, String> config = new HashMap<>();
    // Later put/putAll calls override earlier ones for the same key; this is call-order
    // precedence, unrelated to HashMap's (unspecified) iteration order.
    config.putAll(buildStorageProperties(catalog.getProperties()));
    config.put(TRINO_ICEBERG_REST_VENDED_CREDENTIALS, "true");
    if (gravitinoConfig.isForwardUser()) {
      config.put(TRINO_ICEBERG_REST_SESSION, "USER");
    }
    // The catalog's own trino.bypass properties override the defaults above, so that a Trino
    // release renaming one of them can be worked around without a connector change.
    config.putAll(super.gravitinoToEngineProperties(catalog.getProperties()));
    // The IRC's own authentication is a cluster-level operational setting, so it takes precedence
    // over anything set on a single catalog.
    config.putAll(gravitinoConfig.getIcebergRestCatalogConfig());

    warnOnReservedOverrides(catalog, config);

    config.put(TRINO_ICEBERG_CATALOG_TYPE, TRINO_ICEBERG_CATALOG_TYPE_REST);
    config.put(TRINO_ICEBERG_REST_URI, restUri);
    // The Iceberg client selects the catalog twice over: `warehouse` is the query parameter of the
    // initial GET /v1/config that discovers the catalog, and `prefix` is the path segment of every
    // request after it. Setting only the latter leaves the config call without a catalog name.
    // The server echoes `prefix` back as a config default, so setting it here is belt-and-braces.
    config.put(TRINO_ICEBERG_REST_WAREHOUSE, catalog.getName());
    config.put(TRINO_ICEBERG_REST_PREFIX, catalog.getName());
    return config;
  }

  private Map<String, String> buildHiveBackendProperties(Map<String, String> properties) {
    Set<String> missingProperty =
        Sets.difference(HIVE_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for Hive backend: " + missingProperty);
    }

    Map<String, String> hiveProperties = new HashMap<>();
    hiveProperties.put("iceberg.catalog.type", "hive_metastore");
    hiveProperties.put("hive.metastore.uri", properties.get("uri"));
    return hiveProperties;
  }

  private Map<String, String> buildJDBCBackendProperties(Map<String, String> properties) {
    Set<String> missingProperty =
        Sets.difference(JDBC_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for JDBC backend: " + missingProperty);
    }

    Map<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put("iceberg.catalog.type", "jdbc");
    jdbcProperties.put(
        "iceberg.jdbc-catalog.driver-class",
        properties.get(IcebergConstants.GRAVITINO_JDBC_DRIVER));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-url", properties.get(IcebergConstants.URI));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.connection-user",
        properties.get(IcebergConstants.GRAVITINO_JDBC_USER));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.connection-password",
        properties.get(IcebergConstants.GRAVITINO_JDBC_PASSWORD));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.default-warehouse-dir", properties.get(IcebergConstants.WAREHOUSE));

    jdbcProperties.put(
        "iceberg.jdbc-catalog.catalog-name",
        IcebergPropertiesUtils.getCatalogBackendName(properties));
    if (properties.containsKey(IcebergConstants.GRAVITINO_JDBC_SCHEMA_VERSION)) {
      jdbcProperties.put(
          "iceberg.jdbc-catalog.schema-version",
          properties.get(IcebergConstants.GRAVITINO_JDBC_SCHEMA_VERSION));
    }

    return jdbcProperties;
  }

  // Called before the reserved keys (type/uri/warehouse/prefix) are put into `config` below, so
  // any value already present at this point came from the catalog's own properties or bypass
  // config and is about to be silently overwritten; this logs that so it isn't a silent no-op.
  private void warnOnReservedOverrides(GravitinoCatalog catalog, Map<String, String> config) {
    for (String reserved : RESERVED_REST_PROPERTIES) {
      if (config.containsKey(reserved)) {
        LOG.info(
            "Property '{}' set on catalog '{}' is ignored; the connector always derives it when "
                + "routing through the Iceberg REST server.",
            reserved,
            catalog.getName());
      }
    }
  }

  /**
   * Derives the Trino file system config from the catalog's warehouse location. Vended credentials
   * are only consumed by Trino's native file systems, so the native implementation matching the
   * warehouse scheme has to be enabled.
   */
  private Map<String, String> buildStorageProperties(Map<String, String> properties) {
    Map<String, String> storageProperties = new HashMap<>();
    // Always available as the fallback; where a native file system is also enabled below, that one
    // takes precedence for its own scheme.
    storageProperties.put(TRINO_FS_HADOOP_ENABLED, "true");

    String warehouse = properties.get(IcebergConstants.WAREHOUSE);
    if (StringUtils.isBlank(warehouse)) {
      return storageProperties;
    }

    String scheme = StringUtils.substringBefore(warehouse, "://").toLowerCase(Locale.ROOT);
    switch (scheme) {
      case "s3":
      case "s3a":
      case "s3n":
        storageProperties.put(TRINO_FS_NATIVE_S3_ENABLED, "true");
        copyProperty(
            properties, S3Properties.GRAVITINO_S3_REGION, storageProperties, TRINO_S3_REGION);
        copyProperty(
            properties, S3Properties.GRAVITINO_S3_ENDPOINT, storageProperties, TRINO_S3_ENDPOINT);
        // Required by S3-compatible stores such as MinIO and Ceph, which a custom endpoint implies.
        copyProperty(
            properties,
            S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS,
            storageProperties,
            TRINO_S3_PATH_STYLE_ACCESS);
        break;
      case "gs":
        storageProperties.put(TRINO_FS_NATIVE_GCS_ENABLED, "true");
        break;
      case "abfs":
      case "abfss":
      case "wasb":
      case "wasbs":
        storageProperties.put(TRINO_FS_NATIVE_AZURE_ENABLED, "true");
        break;
      default:
        warnOnSchemeWithoutNativeFileSystem(properties, warehouse, scheme);
        break;
    }
    return storageProperties;
  }

  /**
   * Warns when a warehouse scheme has no Trino native file system but the catalog vends
   * credentials. The Hadoop file system cannot consume a vended credential, so table access would
   * fail at read time with a storage authentication error far from its cause.
   */
  private void warnOnSchemeWithoutNativeFileSystem(
      Map<String, String> properties, String warehouse, String scheme) {
    // hdfs and file legitimately run on the Hadoop file system, and a warehouse without a scheme
    // is a plain path. None of them vend credentials.
    if (!warehouse.contains("://") || "hdfs".equals(scheme) || "file".equals(scheme)) {
      return;
    }
    if (StringUtils.isBlank(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS))) {
      return;
    }
    LOG.warn(
        "Warehouse scheme '{}' has no Trino native file system, so the credentials vended by "
            + "'{}' cannot be applied. Table access falls back to the Hadoop file system and may "
            + "fail to authenticate. Schemes with a native file system: s3/s3a/s3n, gs, "
            + "abfs/abfss/wasb/wasbs.",
        scheme,
        properties.get(CredentialConstants.CREDENTIAL_PROVIDERS));
  }

  private void copyProperty(
      Map<String, String> source, String sourceKey, Map<String, String> target, String targetKey) {
    String value = source.get(sourceKey);
    if (StringUtils.isNotBlank(value)) {
      target.put(targetKey, value);
    }
  }

  private Map<String, String> buildRestBackendProperties(Map<String, String> properties) {
    Set<String> missingProperty =
        Sets.difference(REST_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for Rest backend: " + missingProperty);
    }

    Map<String, String> restProperties = new HashMap<>();
    restProperties.put(TRINO_ICEBERG_CATALOG_TYPE, TRINO_ICEBERG_CATALOG_TYPE_REST);
    restProperties.put(TRINO_ICEBERG_REST_URI, properties.get(IcebergConstants.URI));
    if (properties.containsKey(IcebergConstants.WAREHOUSE)) {
      restProperties.put(
          "iceberg.rest-catalog.warehouse", properties.get(IcebergConstants.WAREHOUSE));
    }
    return restProperties;
  }
}
