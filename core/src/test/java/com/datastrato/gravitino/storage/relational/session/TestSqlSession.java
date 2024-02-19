/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.session;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.Config;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.commons.dbcp2.BasicDataSource;
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
      dir.delete();
    }
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
    SqlSessions.closeSqlSession();
    assertNull(SqlSessions.getSessions().get());
  }

  @Test
  public void testOpenAndCommitAndCloseSqlSession() {
    SqlSession session = SqlSessions.getSqlSession();
    assertNotNull(session);
    SqlSessions.commitAndCloseSqlSession();
    assertNull(SqlSessions.getSessions().get());
  }

  @Test
  public void testOpenAndRollbackAndCloseSqlSession() {
    SqlSession session = SqlSessions.getSqlSession();
    assertNotNull(session);
    SqlSessions.rollbackAndCloseSqlSession();
    assertNull(SqlSessions.getSessions().get());
  }
}
