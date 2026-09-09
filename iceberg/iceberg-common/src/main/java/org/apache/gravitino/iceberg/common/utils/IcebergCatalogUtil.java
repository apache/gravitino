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
package org.apache.gravitino.iceberg.common.utils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.iceberg.common.ClosableHiveCatalog;
import org.apache.gravitino.iceberg.common.ClosableJdbcCatalog;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.rest.auth.UserPrincipalForwardingAuthManager;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogWithMetadataLocationSupport;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcCatalogWithMetadataLocationSupport;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.memory.MemoryCatalogWithMetadataLocationSupport;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogUtil.class);

  /**
   * Column that Iceberg adds to the {@code iceberg_tables} control table in its V1 view-support
   * migration (see {@code JdbcUtil} in iceberg-core).
   */
  private static final String ICEBERG_TYPE_COLUMN = "iceberg_type";

  private static final String GCS_CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  private static final ConcurrentHashMap<String, InMemoryCatalog> MEMORY_CATALOGS =
      new ConcurrentHashMap<>();

  private static InMemoryCatalog loadMemoryCatalog(IcebergConfig icebergConfig) {
    String catalogUuid = icebergConfig.getAllConfig().get(IcebergConstants.CATALOG_UUID);
    if (catalogUuid == null) {
      return createMemoryCatalog(icebergConfig);
    }
    return MEMORY_CATALOGS.computeIfAbsent(
        catalogUuid, ignored -> createMemoryCatalog(icebergConfig));
  }

  private static InMemoryCatalog createMemoryCatalog(IcebergConfig icebergConfig) {
    String icebergCatalogName = icebergConfig.getCatalogBackendName();
    InMemoryCatalog memoryCatalog = new MemoryCatalogWithMetadataLocationSupport();
    Map<String, String> resultProperties = icebergConfig.getIcebergCatalogProperties();
    if (!resultProperties.containsKey(IcebergConstants.WAREHOUSE)) {
      resultProperties.put(IcebergConstants.WAREHOUSE, "/tmp");
    }
    applyDefaultResolvingFileIO(resultProperties);
    memoryCatalog.initialize(icebergCatalogName, resultProperties);
    return memoryCatalog;
  }

  /**
   * Removes the in-memory Iceberg catalog associated with a permanently dropped Gravitino catalog.
   *
   * @param catalogUuid the unique Gravitino catalog identifier
   */
  public static void removeMemoryCatalog(String catalogUuid) {
    InMemoryCatalog memoryCatalog = MEMORY_CATALOGS.remove(catalogUuid);
    if (memoryCatalog != null) {
      try {
        memoryCatalog.close();
      } catch (IOException e) {
        LOG.warn("Failed to close dropped in-memory Iceberg catalog {}", catalogUuid, e);
      }
    }
  }

  @VisibleForTesting
  static void clearMemoryCatalogs() {
    MEMORY_CATALOGS.clear();
  }

  private static HiveCatalog loadHiveCatalog(IcebergConfig icebergConfig) {
    ClosableHiveCatalog hiveCatalog = new HiveCatalogWithMetadataLocationSupport();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    String icebergCatalogName = icebergConfig.getCatalogBackendName();

    Map<String, String> properties = icebergConfig.getIcebergCatalogProperties();
    applyDefaultResolvingFileIO(properties);
    properties.forEach(hdfsConfiguration::set);
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
    if (authenticationConfig.isSimpleAuth()) {
      hiveCatalog.setConf(hdfsConfiguration);
      hiveCatalog.initialize(icebergCatalogName, properties);
      return hiveCatalog;
    } else if (authenticationConfig.isKerberosAuth()) {
      Map<String, String> resultProperties = new HashMap<>(properties);
      resultProperties.put(CatalogProperties.CLIENT_POOL_CACHE_KEYS, "USER_NAME");
      hdfsConfiguration.set(HADOOP_SECURITY_AUTHORIZATION, "true");
      hdfsConfiguration.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      hiveCatalog.setConf(hdfsConfiguration);
      hiveCatalog.initialize(icebergCatalogName, resultProperties);
      return hiveCatalog;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported authentication method: " + authenticationConfig.getAuthType());
    }
  }

  @SuppressWarnings("FormatStringAnnotation")
  private static JdbcCatalog loadJdbcCatalog(IcebergConfig icebergConfig) {
    String driverClassName = icebergConfig.getJdbcDriver();
    String icebergCatalogName = icebergConfig.getCatalogBackendName();

    Map<String, String> properties = icebergConfig.getIcebergCatalogProperties();
    applyDefaultResolvingFileIO(properties);
    try {
      // Load the jdbc driver
      Class.forName(driverClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Couldn't load jdbc driver " + driverClassName);
    }
    ClosableJdbcCatalog jdbcCatalog =
        new JdbcCatalogWithMetadataLocationSupport(
            icebergConfig.get(IcebergConfig.JDBC_INIT_TABLES));

    // Default to V1 schema to support view operations; can be overridden by explicit config.
    properties.putIfAbsent(IcebergConstants.ICEBERG_JDBC_SCHEMA_VERSION, "V1");

    // Default to strict mode so that creating a table or view in a non-existent namespace fails
    // with NoSuchNamespaceException (HTTP 404) instead of implicitly creating the namespace,
    // matching the Iceberg REST spec and the memory backend behavior. Can be overridden by
    // explicit config.
    properties.putIfAbsent(IcebergConstants.ICEBERG_JDBC_STRICT_MODE, "true");

    // Add SQLSTATE 08S01 (Communication link failure) to retryable status codes so that
    // idle connections dropped by MySQL wait_timeout are automatically retried instead of
    // failing with CommunicationsException.
    String existing = properties.putIfAbsent("retryable_status_codes", "08S01");
    if (existing != null && !existing.contains("08S01")) {
      properties.put("retryable_status_codes", existing + ",08S01");
    }

    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
    if (authenticationConfig.isKerberosAuth()) {
      hdfsConfiguration.set(HADOOP_SECURITY_AUTHORIZATION, "true");
      hdfsConfiguration.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    } else if (!authenticationConfig.isSimpleAuth()) {
      throw new UnsupportedOperationException(
          "Unsupported authentication method: " + authenticationConfig.getAuthType());
    }

    try {
      jdbcCatalog.setConf(hdfsConfiguration);
      jdbcCatalog.initialize(icebergCatalogName, properties);
    } catch (UncheckedSQLException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException
          && cause.getMessage() != null
          && cause.getMessage().contains("Access denied")) {
        throw new ConnectionFailedException(e, e.getMessage());
      }
      if (!isConcurrentViewMigrationConflict(e)) {
        throw e;
      }
      // Iceberg's V1 view-support migration adds the `iceberg_type` column to the shared
      // `iceberg_tables` control table with a non-idempotent `ALTER TABLE ... ADD COLUMN`. When
      // several Iceberg JDBC catalogs share the same backend `uri` (hence one `iceberg_tables`), a
      // losing racer finds the column already added by another catalog and fails initialization.
      // The schema is already at V1, so re-initialize a fresh catalog once: the column now exists,
      // Iceberg skips the migration, and initialization completes.
      LOG.info(
          "iceberg_type column already added by another Iceberg JDBC catalog sharing the same "
              + "backend uri; re-initializing catalog {}",
          icebergCatalogName);
      jdbcCatalog =
          new JdbcCatalogWithMetadataLocationSupport(
              icebergConfig.get(IcebergConfig.JDBC_INIT_TABLES));
      jdbcCatalog.setConf(hdfsConfiguration);
      jdbcCatalog.initialize(icebergCatalogName, properties);
    }
    return jdbcCatalog;
  }

  /**
   * Whether an {@link UncheckedSQLException} from {@code JdbcCatalog.initialize} is the benign
   * conflict raised when another Iceberg JDBC catalog sharing the same backend {@code uri} already
   * ran Iceberg's V1 view-support migration.
   *
   * <p>That migration adds the {@value #ICEBERG_TYPE_COLUMN} column to the shared {@code
   * iceberg_tables} table with a non-idempotent {@code ALTER TABLE ... ADD COLUMN}, so a losing
   * racer fails with a duplicate-column error whose wording differs per database (MySQL: {@code
   * Duplicate column name 'iceberg_type'}; PostgreSQL: {@code column "iceberg_type" ... already
   * exists}; SQLite: {@code duplicate column name: iceberg_type}).
   *
   * @param e the exception thrown by catalog initialization
   * @return {@code true} if the failure is a duplicate {@value #ICEBERG_TYPE_COLUMN} column
   *     conflict
   */
  @VisibleForTesting
  static boolean isConcurrentViewMigrationConflict(UncheckedSQLException e) {
    Throwable cause = e.getCause();
    if (!(cause instanceof SQLException) || cause.getMessage() == null) {
      return false;
    }
    String message = cause.getMessage().toLowerCase(Locale.ROOT);
    return message.contains(ICEBERG_TYPE_COLUMN)
        && (message.contains("duplicate column") || message.contains("already exists"));
  }

  private static Catalog loadRestCatalog(IcebergConfig icebergConfig) {
    String icebergCatalogName = icebergConfig.getCatalogBackendName();
    RESTCatalog restCatalog = new RESTCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    Map<String, String> properties = Maps.newHashMap(icebergConfig.getIcebergCatalogProperties());
    applyDefaultResolvingFileIO(properties);

    // REST catalog must use forward access token from the user request
    properties.put(AuthProperties.AUTH_TYPE, UserPrincipalForwardingAuthManager.class.getName());
    applyRestCatalogHttpTimeoutProperties(icebergConfig, properties);

    properties.forEach(hdfsConfiguration::set);
    restCatalog.setConf(hdfsConfiguration);
    restCatalog.initialize(icebergCatalogName, properties);
    return restCatalog;
  }

  private static Catalog loadCustomCatalog(IcebergConfig icebergConfig) {
    String customCatalogName = icebergConfig.getCatalogBackendName();
    String className = icebergConfig.get(IcebergConfig.CATALOG_BACKEND_IMPL);
    Map<String, String> properties = icebergConfig.getIcebergCatalogProperties();
    applyDefaultResolvingFileIO(properties);
    return CatalogUtil.loadCatalog(
        className, customCatalogName, properties, new HdfsConfiguration());
  }

  @VisibleForTesting
  public static void applyDefaultResolvingFileIO(Map<String, String> properties) {
    properties.putIfAbsent(IcebergConstants.IO_IMPL, ResolvingFileIO.class.getName());
    applyGcsServiceAccountCredentials(properties);
  }

  /**
   * When {@code gcs-service-account-file} is set, mint an OAuth2 access token and inject Iceberg
   * {@code gcs.oauth2.token} / {@code gcs.oauth2.token-expires-at} so the built-in {@code
   * GCSFileIO} can authenticate. Iceberg's FileIO does not understand Gravitino's
   * service-account-file property; S3/OSS/ADLS instead map static keys directly via {@link
   * org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils}.
   *
   * <p>Skips injection when {@code gcs.oauth2.token} is already present. Disables Iceberg's
   * credentials-endpoint refresh because that path is for vended table credentials, not catalog
   * bootstrap from a service account file.
   *
   * @param properties Iceberg catalog properties, mutated in place
   */
  @VisibleForTesting
  static void applyGcsServiceAccountCredentials(Map<String, String> properties) {
    String serviceAccountFile = properties.get(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);
    if (StringUtils.isBlank(serviceAccountFile)) {
      return;
    }
    if (StringUtils.isNotBlank(properties.get(IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN))) {
      return;
    }

    AccessToken accessToken = loadAccessTokenFromFile(serviceAccountFile);
    if (accessToken == null || StringUtils.isBlank(accessToken.getTokenValue())) {
      throw new IllegalStateException(
          "Failed to obtain GCS access token from service account file: " + serviceAccountFile);
    }

    properties.put(IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN, accessToken.getTokenValue());
    Date expirationTime = accessToken.getExpirationTime();
    if (expirationTime != null) {
      properties.put(
          IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
          String.valueOf(expirationTime.toInstant().toEpochMilli()));
    }
    properties.put(IcebergConstants.ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, "false");
    LOG.info(
        "Injected {} from {} for Iceberg GCSFileIO",
        IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN,
        GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);
  }

  /**
   * Returns an {@link IcebergConfig} that includes a minted GCS OAuth2 token when {@code
   * gcs-service-account-file} is configured. The returned config retains {@code
   * gcs.oauth2.token-expires-at} so callers (for example the IRC catalog cache) can expire the
   * catalog before the token becomes invalid.
   *
   * @param icebergConfig original catalog config
   * @return the same instance when no token is injected; otherwise a new config with token fields
   */
  public static IcebergConfig withGcsServiceAccountCredentials(IcebergConfig icebergConfig) {
    Map<String, String> properties = new HashMap<>(icebergConfig.getAllConfig());
    applyGcsServiceAccountCredentials(properties);
    if (properties.equals(icebergConfig.getAllConfig())) {
      return icebergConfig;
    }
    return new IcebergConfig(properties);
  }

  private static AccessToken loadAccessTokenFromFile(String serviceAccountFile) {
    Path credentialsFilePath = Paths.get(serviceAccountFile);
    try (InputStream inputStream = Files.newInputStream(credentialsFilePath)) {
      GoogleCredentials credentials =
          GoogleCredentials.fromStream(inputStream).createScoped(GCS_CLOUD_PLATFORM_SCOPE);
      credentials.refreshIfExpired();
      return credentials.getAccessToken();
    } catch (NoSuchFileException e) {
      throw new UncheckedIOException(
          "GCS service account file does not exist: " + serviceAccountFile, e);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to load GCS service account file: " + serviceAccountFile, e);
    }
  }

  @VisibleForTesting
  static void applyRestCatalogHttpTimeoutProperties(
      IcebergConfig icebergConfig, Map<String, String> properties) {
    properties.put(
        IcebergConstants.ICEBERG_REST_CLIENT_CONNECTION_TIMEOUT_MS,
        String.valueOf(
            icebergConfig.get(IcebergConfig.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS)));
    properties.put(
        IcebergConstants.ICEBERG_REST_CLIENT_SOCKET_TIMEOUT_MS,
        String.valueOf(
            icebergConfig.get(IcebergConfig.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS)));
  }

  @VisibleForTesting
  static Catalog loadCatalogBackend(String catalogType) {
    return loadCatalogBackend(
        IcebergCatalogBackend.valueOf(catalogType.toUpperCase(Locale.ROOT)),
        new IcebergConfig(Collections.emptyMap()));
  }

  public static Catalog loadCatalogBackend(
      IcebergCatalogBackend catalogBackend, IcebergConfig icebergConfig) {
    LOG.info("Load catalog backend of {}", catalogBackend);
    switch (catalogBackend) {
      case MEMORY:
        return loadMemoryCatalog(icebergConfig);
      case HIVE:
        return loadHiveCatalog(icebergConfig);
      case JDBC:
        return loadJdbcCatalog(icebergConfig);
      case REST:
        return loadRestCatalog(icebergConfig);
      case CUSTOM:
        return loadCustomCatalog(icebergConfig);
      default:
        throw new RuntimeException(
            catalogBackend
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogBackend);
    }
  }

  private IcebergCatalogUtil() {}
}
