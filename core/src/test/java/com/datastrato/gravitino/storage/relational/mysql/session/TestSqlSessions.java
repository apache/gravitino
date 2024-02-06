/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mysql.session;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.MYSQL_ENTITY_STORE_DRIVER_NAME_KEY;
import static com.datastrato.gravitino.Configs.MYSQL_ENTITY_STORE_URL_KEY;
import static com.datastrato.gravitino.Configs.MYSQL_ENTITY_STORE_USERNAME_KEY;
import static com.datastrato.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastrato.gravitino.Config;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestSqlSessions {
  private static final String MYSQL_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = MYSQL_STORE_PATH + "/testdb";

  @BeforeAll
  public static void setUp() {
    File dir = new File(DB_DIR);
    if (dir.exists() || !dir.isDirectory()) {
      dir.delete();
    }
    dir.mkdirs();

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.getRawString(MYSQL_ENTITY_STORE_URL_KEY))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.getRawString(MYSQL_ENTITY_STORE_USERNAME_KEY)).thenReturn("sa");
    Mockito.when(config.getRawString(MYSQL_ENTITY_STORE_DRIVER_NAME_KEY))
        .thenReturn("org.h2.Driver");
    SqlSessionFactoryHelper.getInstance().init(config);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      dir.delete();
    }
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
