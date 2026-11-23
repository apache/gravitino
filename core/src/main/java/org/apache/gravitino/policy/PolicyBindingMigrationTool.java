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
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import org.apache.gravitino.storage.relational.database.H2Database;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Command-line entry point for offline policy binding migration. */
public final class PolicyBindingMigrationTool {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyBindingMigrationTool.class);

  private PolicyBindingMigrationTool() {}

  /**
   * Runs the offline migration against the configured metadata store.
   *
   * @param args No arguments; configuration is read from gravitino.conf.
   * @throws Exception If configuration or migration fails.
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 0, "Usage: bin/migrate-policy-bindings.sh [--help]");
    Config config = new Config() {};
    // Config resolves GRAVITINO_CONF_DIR, falling back to GRAVITINO_HOME/conf.
    config.loadFromFile("gravitino.conf");
    Preconditions.checkArgument(
        Configs.RELATIONAL_ENTITY_STORE.equals(config.get(Configs.ENTITY_STORE))
            && Configs.DEFAULT_ENTITY_RELATIONAL_STORE.equals(
                config.get(Configs.ENTITY_RELATIONAL_STORE)),
        "Policy migration requires the JDBC relational entity store");
    String url = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    if (JDBCBackendType.fromURI(url) == JDBCBackendType.H2) {
      url = H2Database.getConnectionUrl(config);
      // Never initialize an empty database when the configured path is wrong.
      url = url.replaceAll("(?i);IFEXISTS=[^;]*", "") + ";IFEXISTS=TRUE";
    }
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, url);
    SqlSessionFactoryHelper.getInstance().init(config);
    SQLExceptionConverterFactory.initConverter(config);
    try (EntityStore entityStore =
        RelationalEntityStore.forOfflineMigration(new JDBCBackend(), config)) {
      SessionUtils.beginTransaction();
      try {
        Connection connection = SqlSessions.currentConnection();
        LOG.info(
            "Migrated {} legacy policy bindings",
            new PolicyBindingMigration(connection, entityStore).migrate());
      } finally {
        SessionUtils.rollbackTransaction();
      }
    }
  }
}
