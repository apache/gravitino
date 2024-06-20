/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedJDBCBackend extends JDBCBackend {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EmbeddedJDBCBackendType.class.getName());

  @Override
  public void initialize(Config config) {
    initEmbeddedJDBCBackend(config);
    super.initialize(config);
  }

  enum EmbeddedJDBCBackendType {
    H2("h2"),
    DERBY("derby"),
    SQLITE("sqlite");

    private final String name;

    EmbeddedJDBCBackendType(String name) {
      this.name = name;
    }

    public static EmbeddedJDBCBackendType fromName(String name) {
      for (EmbeddedJDBCBackendType backend : EmbeddedJDBCBackendType.values()) {
        if (backend.name.equalsIgnoreCase(name)) {
          return backend;
        }
      }

      throw new IllegalArgumentException("Unknown EmbeddedJDBCBackend: " + name);
    }
  }

  private static void initEmbeddedJDBCBackend(Config config) {
    String embeddedJDBCUserName = "gravitino";
    String embeddedJDBCPassword = "gravitino";
    String storagePath = getStoragePath(config);

    EmbeddedJDBCBackendType embeddedJDBCBackend =
        EmbeddedJDBCBackendType.fromName(
            config.get(Configs.ENTRY_RELATIONAL_STORE_EMBEDDED_IMPLEMENTATION));

    switch (embeddedJDBCBackend) {
      case H2:
        initH2Backend(config, embeddedJDBCUserName, embeddedJDBCPassword, storagePath);
        break;
      case DERBY:
      case SQLITE:
        throw new UnsupportedOperationException(embeddedJDBCPassword + " is not supported yet.");
      default:
        throw new IllegalArgumentException("Unknown EmbeddedJDBCBackend: " + embeddedJDBCBackend);
    }
  }

  private static void initH2Backend(
      Config config, String h2UserName, String h2UserPassword, String storagePath) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String connectionUrl =
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", storagePath);
    try (Connection connection =
            DriverManager.getConnection(connectionUrl, h2UserName, h2UserPassword);
        Statement statement = connection.createStatement()) {
      String scriptPath = gravitinoHome + "/scripts/h2/schema-h2.sql";

      StringBuilder ddlBuilder = new StringBuilder();
      IOUtils.readLines(
              Objects.requireNonNull(FileUtils.openInputStream(new File(scriptPath))),
              StandardCharsets.UTF_8)
          .forEach(line -> ddlBuilder.append(line).append("\n"));

      String sqlContent =
          FileUtils.readFileToString(
              new File(gravitinoHome + "/scripts/h2/schema-h2.sql"), StandardCharsets.UTF_8);

      statement.execute(sqlContent);
    } catch (Exception e) {
      LOGGER.error("Failed to create table for H2 database.", e);
      throw new RuntimeException("Failed to create table for H2 database.", e);
    }

    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, connectionUrl);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, h2UserName);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, h2UserPassword);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
  }

  static String getStoragePath(Config config) {
    String dbPath = config.get(Configs.ENTRY_RELATIONAL_JDBC_BACKEND_PATH);
    if (StringUtils.isBlank(dbPath)) {
      return Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_PATH;
    }

    Path path = Paths.get(dbPath);
    // Relative Path
    if (!path.isAbsolute()) {
      path = Paths.get(System.getenv("GRAVITINO_HOME"), dbPath);
      return path.toString();
    }

    return dbPath;
  }
}
