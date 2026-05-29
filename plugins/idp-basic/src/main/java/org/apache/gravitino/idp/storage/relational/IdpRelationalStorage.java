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
package org.apache.gravitino.idp.storage.relational;

import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.idp.storage.relational.converters.IdpSQLExceptionConverterFactory;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.JDBCDatabase;
import org.apache.gravitino.storage.relational.database.H2Database;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/** JDBC bootstrap for built-in IdP relational storage. */
public final class IdpRelationalStorage implements Closeable {

  private static final Map<JDBCBackendType, String> EMBEDDED_JDBC_DATABASE_MAP =
      ImmutableMap.of(JDBCBackendType.H2, H2Database.class.getCanonicalName());

  private JDBCDatabase jdbcDatabase;

  /**
   * Initializes the JDBC session factory and optional embedded database.
   *
   * @param config The server configuration.
   */
  public IdpRelationalStorage(Config config) {
    jdbcDatabase = startEmbeddedDatabaseIfNecessary(config);
    SqlSessionFactoryHelper.getInstance().init(config);
    IdpSQLExceptionConverterFactory.initConverter(config);
  }

  @Override
  public void close() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
    IdpSQLExceptionConverterFactory.close();
    if (jdbcDatabase != null) {
      jdbcDatabase.close();
      jdbcDatabase = null;
    }
  }

  private JDBCDatabase startEmbeddedDatabaseIfNecessary(Config config) {
    String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    JDBCBackendType jdbcBackendType = JDBCBackendType.fromURI(jdbcUrl);
    if (jdbcBackendType != JDBCBackendType.H2) {
      return null;
    }

    try {
      JDBCDatabase database =
          (JDBCDatabase)
              Class.forName(EMBEDDED_JDBC_DATABASE_MAP.get(jdbcBackendType))
                  .getDeclaredConstructor()
                  .newInstance();
      database.initialize(config);
      return database;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create and initialize IdP JDBC backend.", e);
    }
  }
}
