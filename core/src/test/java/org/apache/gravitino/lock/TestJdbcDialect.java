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

package org.apache.gravitino.lock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcDialect {

  @Test
  void fromUrlMatchesPostgresPrefix() {
    Assertions.assertEquals(
        JdbcDialect.POSTGRES, JdbcDialect.fromUrl("jdbc:postgresql://localhost:5432/gravitino"));
    Assertions.assertEquals(JdbcDialect.POSTGRES, JdbcDialect.fromUrl("JDBC:POSTGRESQL://host/db"));
  }

  @Test
  void fromUrlMatchesMysqlPrefix() {
    Assertions.assertEquals(
        JdbcDialect.MYSQL, JdbcDialect.fromUrl("jdbc:mysql://localhost:3306/gravitino"));
  }

  @Test
  void fromUrlMatchesH2Prefix() {
    Assertions.assertEquals(JdbcDialect.H2, JdbcDialect.fromUrl("jdbc:h2:mem:test"));
    Assertions.assertEquals(JdbcDialect.H2, JdbcDialect.fromUrl("jdbc:h2:file:/tmp/gravitino"));
  }

  @Test
  void fromUrlRejectsUnknownPrefix() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JdbcDialect.fromUrl("jdbc:oracle:thin:@host:1521"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JdbcDialect.fromUrl("not-a-jdbc-url"));
  }

  @Test
  void fromUrlRejectsEmpty() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> JdbcDialect.fromUrl(""));
    Assertions.assertThrows(IllegalArgumentException.class, () -> JdbcDialect.fromUrl(null));
  }

  @Test
  void createTableSqlReferencesLockTable() {
    for (JdbcDialect d : JdbcDialect.values()) {
      String sql = d.createTableSql();
      Assertions.assertTrue(sql.contains(JdbcDialect.LOCK_TABLE), "missing table name in: " + sql);
      Assertions.assertTrue(sql.contains("lock_path"), "missing column in: " + sql);
      Assertions.assertTrue(
          sql.toUpperCase(java.util.Locale.ROOT).contains("PRIMARY KEY"),
          "missing PRIMARY KEY in: " + sql);
    }
  }

  @Test
  void lockTimeoutStatementHonorsDialectUnits() {
    Assertions.assertTrue(JdbcDialect.POSTGRES.lockTimeoutStatement(15_000L).contains("15000ms"));
    // MySQL uses seconds, integer; expect 15 (15000ms / 1000) or floor.
    Assertions.assertTrue(JdbcDialect.MYSQL.lockTimeoutStatement(15_000L).contains("15"));
    Assertions.assertTrue(JdbcDialect.H2.lockTimeoutStatement(15_000L).contains("15000"));
  }

  @Test
  void lockTimeoutStatementClampsMysqlToOneSecondMinimum() {
    // 500 ms / 1000 = 0, but innodb_lock_wait_timeout requires >= 1.
    String sql = JdbcDialect.MYSQL.lockTimeoutStatement(500L);
    Assertions.assertTrue(sql.endsWith(" 1"), "expected clamped to 1 second, got: " + sql);
  }

  @Test
  void lockTimeoutStatementRejectsNonPositive() {
    for (JdbcDialect d : JdbcDialect.values()) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> d.lockTimeoutStatement(0));
      Assertions.assertThrows(IllegalArgumentException.class, () -> d.lockTimeoutStatement(-1));
    }
  }

  @Test
  void upsertSqlIsDialectSpecific() {
    Assertions.assertTrue(JdbcDialect.POSTGRES.upsertSql().contains("ON CONFLICT DO NOTHING"));
    Assertions.assertTrue(JdbcDialect.MYSQL.upsertSql().contains("INSERT IGNORE"));
    Assertions.assertTrue(JdbcDialect.H2.upsertSql().startsWith("MERGE INTO"));
  }

  @Test
  void selectForShareSqlDegradesOnH2() {
    // H2's FOR SHARE is unreliable; backend degrades to FOR UPDATE.
    Assertions.assertEquals(
        JdbcDialect.H2.selectForUpdateSql(), JdbcDialect.H2.selectForShareSql());
    Assertions.assertTrue(JdbcDialect.POSTGRES.selectForShareSql().endsWith("FOR SHARE"));
    Assertions.assertTrue(JdbcDialect.MYSQL.selectForShareSql().endsWith("FOR SHARE"));
  }

  @Test
  void selectForUpdateSqlIsUniformAcrossDialects() {
    for (JdbcDialect d : JdbcDialect.values()) {
      Assertions.assertTrue(d.selectForUpdateSql().endsWith("FOR UPDATE"), d.toString());
    }
  }
}
