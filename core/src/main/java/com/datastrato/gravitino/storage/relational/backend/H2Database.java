/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.backend;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.relational.JDBCDatabase;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2Database implements JDBCDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(H2Database.class);

  private static final String H2_DEFAULT_USER_NAME = "gravitino";
  private static final String H2_DEFAULT_PASSWORD = "gravitino";

  @Override
  public void initialize(Config config) throws RuntimeException {
    startH2Backend(config);
  }

  public void startH2Backend(Config config) {

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String storagePath = getStoragePath(config);
    String originalJDBCUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    String connectionUrl = constructH2URI(originalJDBCUrl, storagePath);
    try (Connection connection =
            DriverManager.getConnection(connectionUrl, H2_DEFAULT_USER_NAME, H2_DEFAULT_PASSWORD);
        Statement statement = connection.createStatement()) {
      String sqlContent =
          FileUtils.readFileToString(
              new File(gravitinoHome + "/scripts/h2/schema-h2.sql"), StandardCharsets.UTF_8);

      statement.execute(sqlContent);
    } catch (Exception e) {
      LOG.error("Failed to create table for H2 database.", e);
      throw new RuntimeException("Failed to create table for H2 database.", e);
    }

    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, connectionUrl);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, H2_DEFAULT_USER_NAME);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, H2_DEFAULT_PASSWORD);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
  }

  private static String constructH2URI(String originURI, String storagePath) {
    if (!originURI.contains(":file:")) {
      originURI = "jdbc:h2:file:" + storagePath;
    }

    if (!originURI.contains("DB_CLOSE_DELAY")) {
      originURI = originURI + ";DB_CLOSE_DELAY=-1";
    }

    if (!originURI.contains("MODE")) {
      originURI = originURI + ";MODE=MYSQL";
    }

    return originURI;
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
