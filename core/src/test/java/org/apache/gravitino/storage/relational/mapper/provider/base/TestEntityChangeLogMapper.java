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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
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
    try (Statement statement = sharedSession.getConnection().createStatement()) {
      statement.execute("DELETE FROM entity_change_log");
    }
    sharedSession.clearCache();
  }

  @Test
  void testEntityChangeLogInsertAndSelect() {
    long jvmBefore = System.currentTimeMillis();
    entityChangeLogMapper.insertEntityChange(
        "metalake1", "TABLE", "metalake1.cat.schema.tbl", OperateType.ALTER);
    long jvmAfter = System.currentTimeMillis();

    List<EntityChangeRecord> records =
        entityChangeLogMapper.selectEntityChanges(jvmBefore - 1000L, 10);
    Assertions.assertEquals(1, records.size());
    EntityChangeRecord record = records.get(0);
    Assertions.assertEquals("metalake1", record.getMetalakeName());
    Assertions.assertEquals("TABLE", record.getEntityType());
    Assertions.assertEquals("metalake1.cat.schema.tbl", record.getFullName());
    Assertions.assertEquals(OperateType.ALTER, record.getOperateType());
    // created_at is set by the DB, so it should be close to JVM time but not exactly equal.
    Assertions.assertTrue(
        record.getCreatedAt() >= jvmBefore - 1000L && record.getCreatedAt() <= jvmAfter + 1000L,
        "expected DB-time created_at within 1s of JVM clock, got " + record.getCreatedAt());
    Assertions.assertTrue(record.getId() > 0L);
  }

  @Test
  void testEntityChangeLogPruneOldEntries() throws SQLException {
    entityChangeLogMapper.insertEntityChange(
        "metalake1", "SCHEMA", "metalake1.cat.schema", OperateType.ALTER);
    forceCreatedAt("metalake1.cat.schema", 1000L);
    entityChangeLogMapper.insertEntityChange(
        "metalake1", "TABLE", "metalake1.cat.schema.tbl", OperateType.DROP);
    long recent =
        entityChangeLogMapper.selectEntityChanges(0L, 100).stream()
            .filter(r -> r.getFullName().equals("metalake1.cat.schema.tbl"))
            .mapToLong(EntityChangeRecord::getCreatedAt)
            .findFirst()
            .orElseThrow(() -> new AssertionError("recent row missing"));

    entityChangeLogMapper.pruneOldEntityChanges(1001L);

    List<EntityChangeRecord> after = entityChangeLogMapper.selectEntityChanges(0L, 100);
    Assertions.assertEquals(1, after.size());
    Assertions.assertEquals(recent, after.get(0).getCreatedAt());
  }

  @Test
  void testEntityChangeLogSameTimestampOrderedById() throws SQLException {
    entityChangeLogMapper.insertEntityChange("metalake1", "TABLE", "a", OperateType.ALTER);
    entityChangeLogMapper.insertEntityChange("metalake1", "TABLE", "b", OperateType.ALTER);
    entityChangeLogMapper.insertEntityChange("metalake1", "TABLE", "c", OperateType.ALTER);
    forceCreatedAt("a", 5_000_000L);
    forceCreatedAt("b", 5_000_000L);
    forceCreatedAt("c", 5_000_000L);

    List<EntityChangeRecord> rows = entityChangeLogMapper.selectEntityChanges(0L, 100);
    Assertions.assertEquals(3, rows.size());
    Assertions.assertTrue(rows.get(0).getId() < rows.get(1).getId());
    Assertions.assertTrue(rows.get(1).getId() < rows.get(2).getId());
  }

  private void forceCreatedAt(String fullName, long createdAt) throws SQLException {
    try (PreparedStatement statement =
        sharedSession
            .getConnection()
            .prepareStatement(
                "UPDATE entity_change_log SET created_at = ? WHERE entity_full_name = ?")) {
      statement.setLong(1, createdAt);
      statement.setString(2, fullName);
      statement.executeUpdate();
    }
    sharedSession.clearCache();
  }
}
