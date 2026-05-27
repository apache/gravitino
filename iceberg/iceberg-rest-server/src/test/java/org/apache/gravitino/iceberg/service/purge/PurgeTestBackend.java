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

package org.apache.gravitino.iceberg.service.purge;

import java.sql.Connection;
import java.sql.Statement;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.mockito.Mockito;

/**
 * Shared in-memory H2 backend for purge unit tests. Initializes the entity store's singleton {@link
 * SqlSessionFactoryHelper} once (so the store-under-test exercises the real {@code SessionUtils}
 * path and registered mapper) and creates the {@code iceberg_cleanup_job} table.
 */
final class PurgeTestBackend {

  private PurgeTestBackend() {}

  /**
   * Ensures the shared {@link SqlSessionFactoryHelper} points at an in-memory H2 backend with the
   * {@code iceberg_cleanup_job} table. Idempotent and resilient to the factory having been closed
   * or repointed by another test class (e.g. the docker-tagged multi-database IT).
   */
  static synchronized void init() {
    if (!factoryInitialized()) {
      Config config = Mockito.mock(Config.class);
      Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn("relational");
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE)).thenReturn("h2");
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL))
          .thenReturn("jdbc:h2:mem:iceberg_purge_test;DB_CLOSE_DELAY=-1;MODE=MYSQL");
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("sa");
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("");
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
          .thenReturn("org.h2.Driver");
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS))
          .thenReturn(20);
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
          .thenReturn(1000L);

      SqlSessionFactoryHelper.getInstance().init(config);
    }
    createSchema();
    clear();
  }

  private static boolean factoryInitialized() {
    try {
      SqlSessionFactoryHelper.getInstance().getSqlSessionFactory();
      return true;
    } catch (IllegalStateException notInitialized) {
      return false;
    }
  }

  /** Removes all rows so each test starts from a clean table. */
  static void clear() {
    execute("DELETE FROM iceberg_cleanup_job");
  }

  private static void createSchema() {
    for (String ddl : PurgeTestSchema.H2_CREATE.split(";")) {
      if (!ddl.trim().isEmpty()) {
        execute(ddl);
      }
    }
  }

  private static void execute(String sql) {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute purge test DDL: " + sql, e);
    }
  }
}
