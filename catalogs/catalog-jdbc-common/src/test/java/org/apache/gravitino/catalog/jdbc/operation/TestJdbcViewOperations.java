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
package org.apache.gravitino.catalog.jdbc.operation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcViewOperations {

  private static final class TestViewOperations extends JdbcViewOperations {
    @Override
    protected String getSqlDialect() {
      return "test";
    }

    @Override
    protected String quoteIdentifier(String identifier) {
      return "\"" + identifier + "\"";
    }

    @Override
    protected String loadViewDefinition(
        java.sql.Connection connection, String databaseName, String viewName) {
      return "SELECT 1";
    }
  }

  @Test
  public void testExtractSelectBodyFromCreateView() {
    TestViewOperations operations = new TestViewOperations();
    String createView =
        "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` VIEW `v` AS select `id` from `t`";
    Assertions.assertEquals(
        "select `id` from `t`", operations.extractSelectBodyFromCreateView(createView));
  }

  @Test
  public void testExtractSelectBodyFromCreateViewWithoutAsReturnsOriginal() {
    TestViewOperations operations = new TestViewOperations();
    String sql = "SELECT 1";
    Assertions.assertEquals("SELECT 1", operations.extractSelectBodyFromCreateView(sql));
  }
}
