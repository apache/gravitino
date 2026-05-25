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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link MysqlViewOperations}. */
public class TestMysqlViewOperations {

  private MysqlViewOperations ops;

  @BeforeEach
  void setUp() {
    ops = new MysqlViewOperations();
  }

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
  public void testGenerateListViewsSqlIsParameterized() {
    String sql = ops.generateListViewsSql();
    Assertions.assertTrue(sql.contains("?"), "SQL should contain parameter placeholder");
    Assertions.assertTrue(
        sql.toLowerCase().contains("information_schema"), "SQL should query information_schema");
  }

  @Test
  public void testGenerateLoadViewSqlIsParameterized() {
    String sql = ops.generateLoadViewSql();
    Assertions.assertTrue(sql.contains("?"), "SQL should contain parameter placeholder");
    Assertions.assertTrue(
        sql.toLowerCase().contains("view_definition"), "SQL should select VIEW_DEFINITION");
  }

  @Test
  public void testGenerateCreateViewSql() {
    String sql = ops.generateCreateViewSql("my_view", "SELECT 1");
    Assertions.assertTrue(sql.contains("CREATE VIEW"), "Should contain CREATE VIEW");
    Assertions.assertTrue(sql.contains("`my_view`"), "Should quote view name with backticks");
    Assertions.assertTrue(sql.contains("SELECT 1"), "Should contain the view definition");
  }

  @Test
  public void testGenerateReplaceViewSql() {
    String sql = ops.generateReplaceViewSql("my_view", "SELECT 2");
    Assertions.assertTrue(
        sql.contains("CREATE OR REPLACE VIEW"), "Should contain CREATE OR REPLACE VIEW");
    Assertions.assertTrue(sql.contains("`my_view`"), "Should quote view name");
  }

  @Test
  public void testGenerateRenameViewSql() {
    String sql = ops.generateRenameViewSql("old_view", "new_view");
    Assertions.assertTrue(sql.contains("RENAME TABLE"), "MySQL uses RENAME TABLE for views");
    Assertions.assertTrue(sql.contains("`old_view`"), "Should quote old name");
    Assertions.assertTrue(sql.contains("`new_view`"), "Should quote new name");
  }

  @Test
  public void testGenerateDropViewSql() {
    String sql = ops.generateDropViewSql("my_view");
    Assertions.assertTrue(sql.contains("DROP VIEW"), "Should contain DROP VIEW");
    Assertions.assertTrue(sql.contains("`my_view`"), "Should quote view name");
  }
}
