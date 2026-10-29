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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.database.H2Database;
import org.apache.gravitino.utils.JdbcUrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Command-line entry point for offline policy binding migration. */
public final class PolicyBindingMigrationTool {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyBindingMigrationTool.class);

  private PolicyBindingMigrationTool() {}

  /**
   * Runs the offline migration against the configured metadata store.
   *
   * @param args No arguments to migrate, or one of --dry-run, --verify, --help.
   * @throws Exception If configuration, precheck, migration, or verification fails.
   */
  public static void main(String[] args) throws Exception {
    String mode = mode(args);
    if ("--help".equals(mode)) {
      LOG.info(
          "Usage: bin/migrate-policy-bindings.sh [--dry-run | --verify | --help]."
              + " Stop all servers, back up the database, and apply schema upgrades first.");
      return;
    }
    Config config = new Config() {};
    config.loadFromFile("gravitino.conf");
    Preconditions.checkArgument(
        Configs.RELATIONAL_ENTITY_STORE.equals(config.get(Configs.ENTITY_STORE))
            && Configs.DEFAULT_ENTITY_RELATIONAL_STORE.equals(
                config.get(Configs.ENTITY_RELATIONAL_STORE)),
        "Policy migration requires the JDBC relational entity store");
    String url = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    String driver = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER);
    JdbcUrlUtils.validateJdbcConfig(driver, url, config.getAllConfig());
    if (JDBCBackendType.fromURI(url) == JDBCBackendType.H2) {
      url = H2Database.getConnectionUrl(config);
      // Never initialize an empty database when the configured path is wrong.
      url = url.replaceAll("(?i);IFEXISTS=[^;]*", "") + ";IFEXISTS=TRUE";
    }
    Class.forName(driver);
    try (Connection connection =
        DriverManager.getConnection(
            url,
            config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER),
            config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD))) {
      connection.setAutoCommit(false);
      connection.setReadOnly(!mode.isEmpty());
      try {
        PolicyBindingMigration migration = new PolicyBindingMigration(connection);
        switch (mode) {
          case "--dry-run":
            migration.dryRun();
            break;
          case "--verify":
            migration.verify();
            break;
          default:
            LOG.info("Migrated {} legacy policy bindings", migration.migrate());
        }
      } finally {
        connection.rollback();
      }
    }
  }

  static String mode(String[] args) {
    Preconditions.checkArgument(args.length <= 1, "Expected at most one migration mode");
    String mode = args.length == 0 ? "" : args[0];
    Preconditions.checkArgument(
        mode.isEmpty()
            || "--dry-run".equals(mode)
            || "--verify".equals(mode)
            || "--help".equals(mode),
        "Unknown migration mode: %s",
        mode);
    return mode;
  }
}
