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

import io.trino.jdbc.TrinoDriver;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class dynamically register the Catalog managed by Apache Gravitino into Trino using Trino
 * CREATE CATALOG statement. It allows the catalog to be used in Trino like a regular Trino catalog.
 */
public class CatalogRegister {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogRegister.class);

  private static final int MIN_SUPPORT_TRINO_SPI_VERSION = 435;
  private static final int MAX_SUPPORT_TRINO_SPI_VERSION = 439;
  private static final int MIN_SUPPORT_CATALOG_NAME_WITH_METALAKE_TRINO_SPI_VERSION = 446;
  private static final int EXECUTE_QUERY_MAX_RETRIES = 6;
  private static final int EXECUTE_QUERY_BACKOFF_TIME_SECOND = 5;

  private String trinoVersion;
  private Connection connection;
  private boolean isCoordinator;
  private boolean isStarted = false;
  private String catalogStoreDirectory;
  private GravitinoConfig config;

  private void checkTrinoSpiVersion(ConnectorContext context) {
    this.trinoVersion = context.getSpiVersion();

    int version = Integer.parseInt(trinoVersion);

    if (version < MIN_SUPPORT_TRINO_SPI_VERSION || version > MAX_SUPPORT_TRINO_SPI_VERSION) {
      Boolean skipTrinoVersionValidation = config.isSkipTrinoVersionValidation();
      if (!skipTrinoVersionValidation) {
        String errmsg =
            String.format(
                "Unsupported Trino-%s version. The Supported version for the Gravitino-Trino-connector from Trino-%d to Trino-%d."
                    + "Maybe you can set gravitino.trino.skip-version-validation to skip version validation.",
                trinoVersion, MIN_SUPPORT_TRINO_SPI_VERSION, MAX_SUPPORT_TRINO_SPI_VERSION);
        throw new TrinoException(GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION, errmsg);
      } else {
        LOG.warn(
            "The version %s has not undergone thorough testing with Gravitino, there may be compatiablity problem.",
            trinoVersion);
      }
    }

    isCoordinator = context.getNodeManager().getCurrentNode().isCoordinator();
  }

  private void checkSupportCatalogNameWithMetalake(
      ConnectorContext context, GravitinoConfig config) {
    if (!config.singleMetalakeMode()) {
      int version = Integer.parseInt(context.getSpiVersion());
      if (version < MIN_SUPPORT_CATALOG_NAME_WITH_METALAKE_TRINO_SPI_VERSION) {
        LOG.warn(
            "Trino-{} does not support catalog name with dots, The minimal required version is Trino-{}."
                + "Some errors may occur when using the USE <CATALOG>.<SCHEMA> statement in Trino",
            trinoVersion,
            MIN_SUPPORT_CATALOG_NAME_WITH_METALAKE_TRINO_SPI_VERSION);
      }
    }
  }

  boolean isCoordinator() {
    return isCoordinator;
  }

  boolean isTrinoStarted() {
    if (isStarted) {
      return true;
    }

    String command = "SELECT 1";
    try (Statement statement = connection.createStatement()) {
      isStarted = statement.execute(command);
      return isStarted;
    } catch (Exception e) {
      LOG.warn("Trino server is not started: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Initializes the catalog register with the specified Trino connector context and Gravitino
   * configuration.
   *
   * @param context the Trino connector context
   * @param config the Gravitino configuration
   * @throws Exception if the catalog register fails to initialize
   */
  public void init(ConnectorContext context, GravitinoConfig config) throws Exception {
    this.config = config;
    checkTrinoSpiVersion(context);
    checkSupportCatalogNameWithMetalake(context, config);

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
      LOG.info("Register catalog {} successfully: {}", name, createCatalogCommand);
    } catch (Exception e) {
      String message = String.format("Failed to register catalog %s", name);
      LOG.error(message);
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, message, e);
    }
  }

  private boolean checkCatalogExist(String name) {
    String showCatalogCommand = String.format("SHOW CATALOGS like '%s'", name);
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
            if (catalogName.equals(name) || catalogName.equals("\"" + name + "\"")) {
              return true;
            }
          }
          return false;
        } catch (Exception e) {
          failedException = e;
          LOG.warn("Execute command failed: {}, ", showCatalogCommand, e);
          Thread.sleep(EXECUTE_QUERY_BACKOFF_TIME_SECOND * 1000);
        }
      }
      throw failedException;
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Failed to check catalog exist", e);
    }
  }

  private void executeSql(String sql) {
    try {
      int retries = EXECUTE_QUERY_MAX_RETRIES;
      Exception failedException =
          new TrinoException(
              GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR,
              "Initial placeholder exception - this should be replaced with a real exception if retries fail.");
      while (retries-- > 0) {
        try (Statement statement = connection.createStatement()) {
          // check the catalog is already created
          statement.execute(sql);
          return;
        } catch (Exception e) {
          failedException = e;
          LOG.warn("Execute command failed: {}, ", sql, e);
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
        LOG.warn("Catalog {} does not exist", name);
        return;
      }
      String dropCatalogCommand = generateDropCatalogCommand(name);
      executeSql(dropCatalogCommand);
      LOG.info("Unregister catalog {} successfully: {}", name, dropCatalogCommand);
    } catch (Exception e) {
      String message = String.format("Failed to unregister catalog %s", name);
      LOG.error(message);
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
      LOG.error("Failed to close connection", e);
    }
  }
}
