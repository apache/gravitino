/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.storage.relational.mapper.provider.base;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.auth.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEntityChangeLogMapper {

  private static Path jdbcStorePath;
  private static String dbDir;
  private static JDBCBackend backend;
  private static EntityChangeLogMapper entityChangeLogMapper;
  private static SqlSession sharedSession;

  @BeforeAll
  public static void setup() throws Exception {
    jdbcStorePath = Files.createTempDirectory("gravitino_jdbc_entityChangeLog_");
    Path dbPath = jdbcStorePath.resolve("testdb");
    Files.createDirectories(dbPath);
    dbDir = dbPath.toString();

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn("relational");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE)).thenReturn("h2");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", dbDir));
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD))
        .thenReturn("gravitino");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
        .thenReturn("org.h2.Driver");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(20);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
        .thenReturn(1000L);

    backend = new JDBCBackend();
    backend.initialize(config);

    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    entityChangeLogMapper = sharedSession.getMapper(EntityChangeLogMapper.class);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (sharedSession != null) {
      sharedSession.close();
    }
    if (backend != null) {
      backend.close();
    }
    if (jdbcStorePath != null) {
      FileUtils.deleteDirectory(jdbcStorePath.toFile());
    }
  }

  @BeforeEach
  void truncate() throws SQLException {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("DELETE FROM entity_change_log");
      }
    }
  }

  @Test
  void testEntityChangeLogInsertAndSelect() {
    long now = System.currentTimeMillis();
    entityChangeLogMapper.insertChange(
        "metalake1", "TABLE", "cat.schema.tbl", OperateType.ALTER, now);

    List<EntityChangeRecord> records = entityChangeLogMapper.selectChanges(now - 1, 10);
    Assertions.assertEquals(1, records.size());
    EntityChangeRecord record = records.get(0);
    Assertions.assertEquals("metalake1", record.getMetalakeName());
    Assertions.assertEquals("TABLE", record.getEntityType());
    Assertions.assertEquals("cat.schema.tbl", record.getFullName());
    Assertions.assertEquals(OperateType.ALTER, record.getOperateType());
    Assertions.assertEquals(now, record.getCreatedAt());
    Assertions.assertTrue(record.getId() > 0L);
  }

  @Test
  void testEntityChangeLogPruneOldEntries() {
    long old = 1000L;
    long recent = System.currentTimeMillis();
    entityChangeLogMapper.insertChange(
        "metalake1", "SCHEMA", "cat.schema", OperateType.INSERT, old);
    entityChangeLogMapper.insertChange(
        "metalake1", "TABLE", "cat.schema.tbl", OperateType.DROP, recent);

    entityChangeLogMapper.pruneOldEntries(old + 1);

    List<EntityChangeRecord> after = entityChangeLogMapper.selectChanges(0L, 100);
    Assertions.assertEquals(1, after.size());
    Assertions.assertEquals(recent, after.get(0).getCreatedAt());
  }

  @Test
  void testEntityChangeLogSameTimestampOrderedById() {
    long t = 5_000_000L;
    entityChangeLogMapper.insertChange("metalake1", "TABLE", "a", OperateType.INSERT, t);
    entityChangeLogMapper.insertChange("metalake1", "TABLE", "b", OperateType.INSERT, t);
    entityChangeLogMapper.insertChange("metalake1", "TABLE", "c", OperateType.INSERT, t);

    List<EntityChangeRecord> rows = entityChangeLogMapper.selectChanges(0L, 100);
    Assertions.assertEquals(3, rows.size());
    Assertions.assertTrue(rows.get(0).getId() < rows.get(1).getId());
    Assertions.assertTrue(rows.get(1).getId() < rows.get(2).getId());
  }
}
