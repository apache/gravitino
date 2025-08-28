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

package org.apache.gravitino.storage.relational.session;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestSqlSession {
  private static final String MYSQL_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = MYSQL_STORE_PATH + "/testdb";

  private static Config config;

  @BeforeAll
  public static void setUp() {
    File dir = new File(DB_DIR);
    if (dir.exists() || !dir.isDirectory()) {
      dir.delete();
    }
    dir.mkdirs();

    config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
  }

  @BeforeEach
  public void init() {
    SqlSessionFactoryHelper.getInstance().init(config);
  }

  @AfterEach
  public void cleanUp() {
    SqlSessionFactoryHelper.getInstance().close();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      FileUtils.deleteDirectory(FileUtils.getFile(DB_DIR));
    }

    Files.delete(Paths.get(MYSQL_STORE_PATH));

    SqlSessionFactoryHelper.getInstance().close();
  }

  @Test
  public void testGetInstance() {
    SqlSessionFactoryHelper instance = SqlSessionFactoryHelper.getInstance();
    assertNotNull(instance);
  }

  @Test
  public void testInit() throws SQLException {
    SqlSessionFactoryHelper.getInstance().close();
    SqlSessionFactoryHelper.getInstance().init(config);
    assertNotNull(SqlSessionFactoryHelper.getInstance().getSqlSessionFactory());
    BasicDataSource dataSource =
        (BasicDataSource)
            SqlSessionFactoryHelper.getInstance()
                .getSqlSessionFactory()
                .getConfiguration()
                .getEnvironment()
                .getDataSource();
    assertEquals("org.h2.Driver", dataSource.getDriverClassName());
    assertEquals(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL), dataSource.getUrl());
  }

  @Test
  public void testGetSqlSessionFactoryWithoutInit() {
    SqlSessionFactoryHelper.getInstance().close();
    assertThrows(
        IllegalStateException.class,
        () -> SqlSessionFactoryHelper.getInstance().getSqlSessionFactory());
  }

  @Test
  public void testOpenAndCloseSqlSession() {
    SqlSession session = SqlSessions.getSqlSession();
    assertNotNull(session);
    assertEquals(1, SqlSessions.getSessionCount());
    SqlSessions.closeSqlSession();
    assertNull(SqlSessions.getSessions().get());
    assertEquals(0, SqlSessions.getSessionCount());
  }

  @Test
  public void testOpenAndCommitAndCloseSqlSession() {
    SqlSession session = SqlSessions.getSqlSession();
    assertNotNull(session);
    assertEquals(1, SqlSessions.getSessionCount());
    SqlSessions.commitAndCloseSqlSession();
    assertNull(SqlSessions.getSessions().get());
    assertEquals(0, SqlSessions.getSessionCount());
  }

  @Test
  public void testOpenAndRollbackAndCloseSqlSession() {
    SqlSession session = SqlSessions.getSqlSession();
    assertNotNull(session);
    assertEquals(1, SqlSessions.getSessionCount());
    SqlSessions.rollbackAndCloseSqlSession();
    assertNull(SqlSessions.getSessions().get());
    assertEquals(0, SqlSessions.getSessionCount());
  }
}
