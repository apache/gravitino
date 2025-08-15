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
package org.apache.gravitino.storage.relational.database;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.storage.relational.JDBCDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2Database implements JDBCDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(H2Database.class);
  private String h2ConnectionUri;
  private String username;
  private String password;

  @Override
  public void initialize(Config config) {
    this.h2ConnectionUri = startH2Database(config);
  }

  public String startH2Database(Config config) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String storagePath = getStoragePath(config);
    String originalJDBCUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    this.username = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER);
    this.password = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD);

    String connectionUrl = constructH2URI(originalJDBCUrl, storagePath);

    try (Connection connection = DriverManager.getConnection(connectionUrl, username, password);
        Statement statement = connection.createStatement()) {
      String sqlContent =
          FileUtils.readFileToString(
              new File(
                  gravitinoHome
                      + "/scripts/h2/schema-"
                      + ConfigConstants.CURRENT_SCRIPT_VERSION
                      + "-h2.sql"),
              StandardCharsets.UTF_8);

      statement.execute(sqlContent);
    } catch (Exception e) {
      LOG.error("Failed to create table for H2 database.", e);
      throw new RuntimeException("Failed to create table for H2 database.", e);
    }

    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, connectionUrl);

    return connectionUrl;
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

    if (!originURI.contains("AUTO_SERVER")) {
      originURI = originURI + ";AUTO_SERVER=TRUE";
    }

    return originURI;
  }

  private static String getStoragePath(Config config) {
    String dbPath = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH);
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

  @Override
  public void close() throws IOException {
    try (Connection connection = DriverManager.getConnection(h2ConnectionUri, username, password);
        Statement statement = connection.createStatement()) {
      statement.execute("SHUTDOWN");
    } catch (Exception e) {
      LOG.error("Failed to shutdown H2 database.", e);
      throw new RuntimeException("Failed to shutdown H2 database.", e);
    }
  }
}
