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

package org.apache.gravitino.storage.relational;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;

/** Checks what each {@link ResetStrategy} leaves behind for the next test of the same class. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestResetStrategy extends TestJDBCBackend {

  private long firstId;

  @TestTemplate
  @Order(1)
  void testInsertFirstRow() throws SQLException {
    firstId = insertChangeLogRow();
  }

  @TestTemplate
  @Order(2)
  void testDeleteRowsKeepsCounting() throws SQLException {
    Assertions.assertTrue(insertChangeLogRow() > firstId);
  }

  @TestTemplate
  @Order(3)
  @DatabaseFixture(resetStrategy = ResetStrategy.TRUNCATE_TABLES)
  void testTruncateTablesRestartsCounters() throws SQLException {
    Assertions.assertEquals(firstId, insertChangeLogRow());
  }

  private static long insertChangeLogRow() throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "INSERT INTO entity_change_log"
              + " (metalake_name, entity_type, entity_full_name, operate_type, created_at)"
              + " VALUES ('metalake', 'TABLE', 'table', 3, 0)",
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = statement.getGeneratedKeys()) {
        Assertions.assertTrue(keys.next());
        return keys.getLong(1);
      }
    }
  }
}
