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
package org.apache.gravitino.catalog.mysql.operation;

import org.apache.gravitino.rel.Dialects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for MySQL view operations SQL generation. */
public class TestMysqlViewOperations {

  private final MysqlViewOperations ops = new MysqlViewOperations();

  @Test
  public void testDialectName() {
    Assertions.assertEquals(Dialects.MYSQL, ops.dialectName());
  }

  @Test
  public void testQuoteIdentifier() {
    Assertions.assertEquals("`my_view`", ops.quoteIdentifier("my_view"));
  }

  @Test
  public void testQuoteIdentifierEscapesBacktick() {
    Assertions.assertEquals("`my``view`", ops.quoteIdentifier("my`view"));
  }

  @Test
  public void testGenerateCreateViewSql() {
    String sql = ops.generateCreateViewSql("v1", "SELECT id, name FROM users");
    Assertions.assertEquals("CREATE VIEW `v1` AS SELECT id, name FROM users", sql);
  }

  @Test
  public void testGenerateReplaceViewSql() {
    String sql = ops.generateReplaceViewSql("v1", "SELECT id FROM users");
    Assertions.assertEquals("CREATE OR REPLACE VIEW `v1` AS SELECT id FROM users", sql);
  }

  @Test
  public void testGenerateRenameViewSql() {
    String sql = ops.generateRenameViewSql("old_view", "new_view");
    Assertions.assertEquals("RENAME TABLE `old_view` TO `new_view`", sql);
  }

  @Test
  public void testGenerateDropViewSql() {
    String sql = ops.generateDropViewSql("v1");
    Assertions.assertEquals("DROP VIEW IF EXISTS `v1`", sql);
  }

  @Test
  public void testGenerateListViewsSqlIsParameterized() {
    String sql = ops.generateListViewsSql();
    Assertions.assertTrue(sql.contains("information_schema.VIEWS"));
    Assertions.assertTrue(sql.contains("?"));
    Assertions.assertFalse(sql.contains("'"), "SQL must use ? placeholders, not literals");
  }

  @Test
  public void testGenerateLoadViewSqlIsParameterized() {
    String sql = ops.generateLoadViewSql();
    Assertions.assertTrue(sql.contains("VIEW_DEFINITION"));
    Assertions.assertTrue(sql.contains("?"));
    Assertions.assertFalse(sql.contains("'"), "SQL must use ? placeholders, not literals");
  }
}
