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
package org.apache.gravitino.trino.connector.catalog;

import static org.apache.gravitino.trino.connector.GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR;
import static org.apache.gravitino.trino.connector.GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG;

<<<<<<< HEAD
=======
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
>>>>>>> 52b8f5341 ([#12634] improvement(trino-connector): Log via io.airlift.log.Logger (#12635))
import io.trino.jdbc.TrinoDriver;
import io.trino.spi.TrinoException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * This class dynamically register the Catalog managed by Apache Gravitino into Trino using Trino
 * CREATE CATALOG statement. It allows the catalog to be used in Trino like a regular Trino catalog.
 */
public class CatalogRegister {

  private static final Logger LOG = Logger.get(CatalogRegister.class);

  private static final int EXECUTE_QUERY_MAX_RETRIES = 6;
  private static final int EXECUTE_QUERY_BACKOFF_TIME_SECOND = 5;

  private Connection connection;
  private boolean isStarted = false;
  private String catalogStoreDirectory;
  private GravitinoConfig config;

  boolean isTrinoStarted() {
    if (isStarted) {
      return true;
    }

    String command = "SELECT 1";
    try (Statement statement = connection.createStatement()) {
      isStarted = statement.execute(command);
      return isStarted;
    } catch (Exception e) {
      LOG.warn("Trino server is not started: %s", e.getMessage());
      return false;
    }
  }

  /**
   * Initializes the catalog register with the specified Trino connector context and Gravitino
   * configuration.
   *
   * @param config the Gravitino configuration
   * @throws Exception if the catalog register fails to initialize
   */
  public void init(GravitinoConfig config) throws Exception {
    this.config = config;

    TrinoDriver driver = new TrinoDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put("user", config.getTrinoUser());
    properties.put("password", config.getTrinoPassword());
    try {
      connection = driver.connect(config.getTrinoJdbcURI(), properties);
    } catch (SQLException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR,
          "Failed to initialize the Trino connection.",
          e);
    }

    catalogStoreDirectory = config.getCatalogConfigDirectory();
    if (!Files.exists(Path.of(catalogStoreDirectory))) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
          String.format(
              "Error config for Trino catalog store directory %s, file not found",
              catalogStoreDirectory));
    }
  }

<<<<<<< HEAD
=======
  /**
   * Builds the JDBC properties used by the internal connection to the Trino coordinator.
   *
   * <p>The properties derived from the dedicated {@code trino.jdbc.*} configurations are applied
   * first, then the raw driver properties configured with the {@code trino.jdbc.properties.} prefix
   * are applied on top of them, so that any driver property can be overridden.
   *
   * @param config the Gravitino configuration
   * @return the JDBC properties
   */
  @VisibleForTesting
  static Properties buildJdbcProperties(GravitinoConfig config) {
    boolean sslEnabled = config.isTrinoJdbcSslEnabled();
    String verification = config.getTrinoJdbcSslVerification();
    String truststorePath = config.getTrinoJdbcSslTruststorePath();
    String truststorePassword = config.getTrinoJdbcSslTruststorePassword();
    String truststoreType = config.getTrinoJdbcSslTruststoreType();
    String keystorePath = config.getTrinoJdbcSslKeystorePath();
    String keystorePassword = config.getTrinoJdbcSslKeystorePassword();
    String keystoreType = config.getTrinoJdbcSslKeystoreType();
    String roles = config.getTrinoJdbcRoles();

    validateSslConfig(
        sslEnabled,
        verification,
        truststorePath,
        truststorePassword,
        truststoreType,
        keystorePath,
        keystorePassword,
        keystoreType);

    Properties properties = new Properties();
    properties.put("user", config.getTrinoUser());
    String password = config.getTrinoPassword();
    if (StringUtils.isNotEmpty(password)) {
      properties.put("password", password);
    }

    if (sslEnabled) {
      properties.put("SSL", "true");
      properties.put("SSLVerification", verification);
      if (StringUtils.isNotBlank(truststorePath)) {
        properties.put("SSLTrustStorePath", truststorePath);
      }
      if (StringUtils.isNotEmpty(truststorePassword)) {
        properties.put("SSLTrustStorePassword", truststorePassword);
      }
      if (StringUtils.isNotBlank(truststoreType)) {
        properties.put("SSLTrustStoreType", truststoreType);
      }
      if (StringUtils.isNotBlank(keystorePath)) {
        properties.put("SSLKeyStorePath", keystorePath);
      }
      if (StringUtils.isNotEmpty(keystorePassword)) {
        properties.put("SSLKeyStorePassword", keystorePassword);
      }
      if (StringUtils.isNotBlank(keystoreType)) {
        properties.put("SSLKeyStoreType", keystoreType);
      }
    }

    if (StringUtils.isNotBlank(roles)) {
      properties.put("roles", roles);
    }

    Map<String, String> extraProperties = config.getTrinoJdbcExtraProperties();
    if (!extraProperties.isEmpty()) {
      // Log the names only, the values may contain credentials.
      LOG.debug("Applying extra Trino JDBC properties: %s", extraProperties.keySet());
      extraProperties.keySet().stream()
          .filter(key -> key.startsWith("SSL") && properties.containsKey(key))
          .forEach(
              key ->
                  LOG.warn(
                      "Extra Trino JDBC property '%s' overrides the TLS setting derived from the "
                          + "dedicated configuration and is applied without validation",
                      key));
      properties.putAll(extraProperties);
    }
    return properties;
  }

  private static void validateSslConfig(
      boolean sslEnabled,
      String verification,
      String truststorePath,
      String truststorePassword,
      String truststoreType,
      String keystorePath,
      String keystorePassword,
      String keystoreType) {
    if (!SSL_VERIFICATION_MODES.contains(verification)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          String.format(
              "Invalid value for config 'trino.jdbc.ssl.verification': expected one of %s, got: %s",
              SSL_VERIFICATION_MODES, verification));
    }

    if (!sslEnabled) {
      if (!SSL_VERIFICATION_FULL.equals(verification)) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Config 'trino.jdbc.ssl.verification' requires TLS to be enabled either by an HTTPS "
                + "'discovery.uri' or by 'trino.jdbc.ssl.enabled=true'");
      }
      checkRequiresSslEnabled("trino.jdbc.ssl.truststore.path", truststorePath);
      checkRequiresSslEnabled("trino.jdbc.ssl.truststore.password", truststorePassword);
      checkRequiresSslEnabled("trino.jdbc.ssl.truststore.type", truststoreType);
      checkRequiresSslEnabled("trino.jdbc.ssl.keystore.path", keystorePath);
      checkRequiresSslEnabled("trino.jdbc.ssl.keystore.password", keystorePassword);
      checkRequiresSslEnabled("trino.jdbc.ssl.keystore.type", keystoreType);
      return;
    }

    validateKeystoreConfig(verification, keystorePath, keystorePassword, keystoreType);

    if (StringUtils.isBlank(truststorePath)) {
      // The driver falls back to the default JVM truststore, which the password and the type of a
      // truststore that was never configured have nothing to apply to.
      checkRequires(
          "trino.jdbc.ssl.truststore.password",
          truststorePassword,
          "trino.jdbc.ssl.truststore.path");
      checkRequires(
          "trino.jdbc.ssl.truststore.type", truststoreType, "trino.jdbc.ssl.truststore.path");
      return;
    }

    if (SSL_VERIFICATION_NONE.equals(verification)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "Config 'trino.jdbc.ssl.truststore.path' cannot be used with "
              + "'trino.jdbc.ssl.verification' = NONE");
    }
    if (!Files.exists(Path.of(truststorePath))) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
          String.format(
              "The truststore file configured by 'trino.jdbc.ssl.truststore.path' does not exist: %s",
              truststorePath));
    }
  }

  private static void validateKeystoreConfig(
      String verification, String keystorePath, String keystorePassword, String keystoreType) {
    if (StringUtils.isBlank(keystorePath)) {
      checkRequires(
          "trino.jdbc.ssl.keystore.password", keystorePassword, "trino.jdbc.ssl.keystore.path");
      checkRequires("trino.jdbc.ssl.keystore.type", keystoreType, "trino.jdbc.ssl.keystore.path");
      return;
    }
    if (SSL_VERIFICATION_NONE.equals(verification)) {
      // The driver rejects the keystore properties in this combination, so fail with a config
      // error here rather than letting it surface as a connection failure.
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "Config 'trino.jdbc.ssl.keystore.path' cannot be used with "
              + "'trino.jdbc.ssl.verification' = NONE");
    }
    if (!Files.exists(Path.of(keystorePath))) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
          String.format(
              "The keystore file configured by 'trino.jdbc.ssl.keystore.path' does not exist: %s",
              keystorePath));
    }
  }

  private static void checkRequires(String key, String value, String requiredKey) {
    if (StringUtils.isNotEmpty(value)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          String.format("Config '%s' requires '%s' to be set", key, requiredKey));
    }
  }

  private static void checkRequiresSslEnabled(String key, String value) {
    if (StringUtils.isNotEmpty(value)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          String.format(
              "Config '%s' requires TLS to be enabled either by an HTTPS 'discovery.uri' or by "
                  + "'trino.jdbc.ssl.enabled=true'",
              key));
    }
  }

>>>>>>> 52b8f5341 ([#12634] improvement(trino-connector): Log via io.airlift.log.Logger (#12635))
  private String generateCreateCatalogCommand(String name, GravitinoCatalog gravitinoCatalog)
      throws Exception {
    return String.format(
        "CREATE CATALOG %s USING gravitino WITH ( \"%s\" = 'true', \"%s\" = '%s', %s)",
        name,
        GRAVITINO_DYNAMIC_CONNECTOR,
        GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG,
        GravitinoCatalog.toJson(gravitinoCatalog),
        config.toCatalogConfig());
  }

  private String generateDropCatalogCommand(String name) {
    return String.format("DROP CATALOG %s", name);
  }

  /**
   * Registers a new catalog with the specified name and Gravitino catalog.
   *
   * @param name the name of the catalog
   * @param catalog the Gravitino catalog
   */
  public void registerCatalog(String name, GravitinoCatalog catalog) {
    try {
      String catalogFileName = String.format("%s/%s.properties", catalogStoreDirectory, name);
      File catalogFile = new File(catalogFileName);
      if (catalogFile.exists()) {
        String catalogContents = Files.readString(catalogFile.toPath());
        if (!catalogContents.contains(GRAVITINO_DYNAMIC_CONNECTOR + "=true")) {
          throw new TrinoException(
              GravitinoErrorCode.GRAVITINO_DUPLICATED_CATALOGS,
              "Catalog already exists, the catalog is not created by Gravitino");
        } else {
          throw new TrinoException(
              GravitinoErrorCode.GRAVITINO_CATALOG_ALREADY_EXISTS,
              String.format(
                  "Catalog %s in metalake %s already exists",
                  catalog.getName(), catalog.getMetalake()));
        }
      }

      if (checkCatalogExist(name)) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_DUPLICATED_CATALOGS,
            "Catalog already exists with unknown reason");
      }
      String createCatalogCommand = generateCreateCatalogCommand(name, catalog);
      executeSql(createCatalogCommand);
      LOG.info("Register catalog %s successfully: %s", name, createCatalogCommand);
    } catch (SQLException e) {
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, e.getMessage(), e);
    } catch (Exception e) {
      String message = String.format("Failed to register catalog %s", name);
      LOG.error(e, message);
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, message, e);
    }
  }

  private boolean checkCatalogExist(String name) {
    String showCatalogCommand = "SHOW CATALOGS";
    Exception failedException = null;
    try {
      int retries = EXECUTE_QUERY_MAX_RETRIES;
      while (retries-- > 0) {
        try (Statement statement = connection.createStatement()) {
          // check the catalog is already created
          statement.execute(showCatalogCommand);
          ResultSet rs = statement.getResultSet();
          while (rs.next()) {
            String catalogName = rs.getString(1);
            // In some Trino version catalog name may be quoted, so we need to check both quoted and
            // unquoted names
            if (name.equals(catalogName)
                || name.equals("\"" + catalogName + "\"")
                || ("\"" + name + "\"").equals(catalogName)) {
              return true;
            }
          }
          return false;
        } catch (SQLException e) {
          throw e;
        } catch (Exception e) {
          failedException = e;
          LOG.warn(e, "Failed to execute command: %s", showCatalogCommand);
          Thread.sleep(EXECUTE_QUERY_BACKOFF_TIME_SECOND * 1000);
        }
      }
      throw failedException;
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Failed to check if catalog exists", e);
    }
  }

  private void executeSql(String sql) {
    try {
      int retries = EXECUTE_QUERY_MAX_RETRIES;
      Exception failedException =
          new TrinoException(
              GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "All retry attempts failed");
      while (retries-- > 0) {
        try (Statement statement = connection.createStatement()) {
          // check the catalog is already created
          statement.execute(sql);
          return;
        } catch (SQLException e) {
          throw e;
        } catch (Exception e) {
          failedException = e;
          LOG.warn(e, "Failed to execute command: %s", sql);
          Thread.sleep(EXECUTE_QUERY_BACKOFF_TIME_SECOND * 1000);
        }
      }
      throw failedException;
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Failed to execute query: " + sql, e);
    }
  }

  /**
   * Unregisters a catalog with the specified name.
   *
   * @param name the name of the catalog
   */
  public void unregisterCatalog(String name) {
    try {
      if (!checkCatalogExist(name)) {
        LOG.warn("Catalog %s does not exist", name);
        return;
      }
      String dropCatalogCommand = generateDropCatalogCommand(name);
      executeSql(dropCatalogCommand);
      LOG.info("Unregister catalog %s successfully: %s", name, dropCatalogCommand);
    } catch (Exception e) {
      String message = String.format("Failed to unregister catalog %s", name);
      LOG.error(e, message);
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, message, e);
    }
  }

  /** Closes the catalog register. */
  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error(e, "Failed to close connection");
    }
  }
}
