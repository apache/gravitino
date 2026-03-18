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
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests for read vs write SqlSession routing. */
public class TestSqlSessionsReadWrite {
  private static final String BASE =
      "/tmp/gravitino_test_rw_" + UUID.randomUUID().toString().replace("-", "");
  private static final String WRITE_DB = BASE + "/write";
  private static final String READ_DB = BASE + "/read";

  private Config config;

  @BeforeEach
  public void setUp() {
    new File(WRITE_DB).mkdirs();
    new File(READ_DB).mkdirs();
    config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", WRITE_DB));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", READ_DB));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_CONNECTIONS))
        .thenReturn(-1);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_WAIT_MILLISECONDS))
        .thenReturn(-1L);
    SqlSessionFactoryHelper.getInstance().init(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
    FileUtils.deleteDirectory(new File(BASE));
  }

  @Test
  public void testFactoriesDifferWhenReadOnlyUrlConfigured() {
    assertNotSame(
        SqlSessionFactoryHelper.getInstance().getWriteSqlSessionFactory(),
        SqlSessionFactoryHelper.getInstance().getReadSqlSessionFactory());
  }

  @Test
  public void testReadPoolInheritsWriteMaxConnectionsAndWaitWhenUnset() {
    BasicDataSource readDs =
        (BasicDataSource)
            SqlSessionFactoryHelper.getInstance()
                .getReadSqlSessionFactory()
                .getConfiguration()
                .getEnvironment()
                .getDataSource();
    assertEquals(100, readDs.getMaxTotal());
    assertEquals(1000L, readDs.getMaxWaitMillis());
  }

  @Test
  public void testReadPoolUsesExplicitMaxConnectionsWhenConfigured() {
    SqlSessionFactoryHelper.getInstance().close();
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_CONNECTIONS))
        .thenReturn(42);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_WAIT_MILLISECONDS))
        .thenReturn(2000L);
    SqlSessionFactoryHelper.getInstance().init(config);
    BasicDataSource readDs =
        (BasicDataSource)
            SqlSessionFactoryHelper.getInstance()
                .getReadSqlSessionFactory()
                .getConfiguration()
                .getEnvironment()
                .getDataSource();
    assertEquals(42, readDs.getMaxTotal());
    assertEquals(2000L, readDs.getMaxWaitMillis());
  }

  @Test
  public void testStandaloneReadUsesReadSessionNotWrite() {
    SessionUtils.getWithoutCommit(
        MetalakeMetaMapper.class,
        mapper -> {
          SqlSession readSession = SqlSessions.getReadSessions().get();
          assertNotNull(readSession);
          assertNull(SqlSessions.getWriteSessions().get());
          return null;
        });
    assertNull(SqlSessions.getReadSessions().get());
  }

  @Test
  public void testReadUnderWriteTransactionUsesWriteSession() {
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper -> {
                  assertNotNull(SqlSessions.getWriteSessions().get());
                  assertNull(SqlSessions.getReadSessions().get());
                  return null;
                }));
  }
}
