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

package org.apache.gravitino.storage.relational.mapper.provider.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestGroupMetaH2Provider {

  private static final String DB_CONNECTION_URL = "jdbc:h2:mem:test;MODE=MYSQL;DB_CLOSE_DELAY=-1";
  private static final String DB_USER = "sa";
  private static final String DB_PASSWORD = "";
  private static final String COLUMN_LABEL_ROLE_NAMES = "roleNames";
  private static final String COLUMN_LABEL_ROLE_IDS = "roleIds";
  private static final String QUERY_PARAM_METALAKE_ID = "#{metalakeId}";

  private static Connection connection;
  private static Statement statement;
  private static GroupMetaH2Provider groupMetaH2Provider;

  @BeforeAll
  static void setUp() throws SQLException {
    connection = DriverManager.getConnection(DB_CONNECTION_URL, DB_USER, DB_PASSWORD);
    statement = connection.createStatement();
    groupMetaH2Provider = new GroupMetaH2Provider();
    createSchema(statement);
  }

  private static void createSchema(Statement statement) throws SQLException {
    statement.execute(
        "CREATE TABLE IF NOT EXISTS group_meta ("
            + "group_id BIGINT, "
            + "group_name VARCHAR(255), "
            + "metalake_id BIGINT, "
            + "audit_info VARCHAR(255), "
            + "current_version BIGINT, "
            + "last_version BIGINT, "
            + "deleted_at BIGINT)");
    statement.execute(
        "CREATE TABLE IF NOT EXISTS role_meta ("
            + "role_id BIGINT, "
            + "role_name VARCHAR(255), "
            + "deleted_at BIGINT)");
    statement.execute(
        "CREATE TABLE IF NOT EXISTS group_role_rel ("
            + "group_id BIGINT, "
            + "role_id BIGINT, "
            + "deleted_at BIGINT)");
  }

  @Test
  void testListExtendedGroupPOsByMetalakeIdWithoutRoles() throws SQLException {
    statement.execute("INSERT INTO group_meta VALUES (1, 'g1', 1, 'audit', 0, 0, 0)");
    String sql =
        groupMetaH2Provider
            .listExtendedGroupPOsByMetalakeId(1L)
            .replace(QUERY_PARAM_METALAKE_ID, "1");
    try (ResultSet rs = statement.executeQuery(sql)) {
      rs.next();
      assertEquals("[]", rs.getString(COLUMN_LABEL_ROLE_NAMES));
      assertEquals("[]", rs.getString(COLUMN_LABEL_ROLE_IDS));
    }
  }

  @Test
  void testListExtendedGroupPOsByMetalakeIdWithRoles() throws SQLException {
    statement.execute("INSERT INTO group_meta VALUES (2, 'g2', 2, 'audit2', 0, 0, 0)");
    statement.execute("INSERT INTO role_meta VALUES (1, 'role1', 0)");
    statement.execute("INSERT INTO role_meta VALUES (2, 'role2', 0)");
    statement.execute("INSERT INTO group_role_rel VALUES (2, 1, 0)");
    statement.execute("INSERT INTO group_role_rel VALUES (2, 2, 0)");

    String sql =
        groupMetaH2Provider
            .listExtendedGroupPOsByMetalakeId(2L)
            .replace(QUERY_PARAM_METALAKE_ID, "2");
    try (ResultSet rs = statement.executeQuery(sql)) {
      rs.next();
      assertEquals("[\"role1\",\"role2\"]", rs.getString(COLUMN_LABEL_ROLE_NAMES));
      assertEquals("[\"1\",\"2\"]", rs.getString(COLUMN_LABEL_ROLE_IDS));
    }
  }

  @Test
  void testListExtendedGroupPOsByMetalakeIdWithInvalidRoles() throws SQLException {
    statement.execute("INSERT INTO group_meta VALUES (3, 'g3', 3, 'audit3', 0, 0, 0)");
    statement.execute("INSERT INTO role_meta VALUES (3, 'role3', 0)");
    statement.execute("INSERT INTO role_meta VALUES (4, '', 0)");
    statement.execute("INSERT INTO role_meta VALUES (5, null, 0)");
    statement.execute("INSERT INTO role_meta VALUES (null, 'role6', 0)");
    statement.execute("INSERT INTO group_role_rel VALUES (3, 3, 0)");
    statement.execute("INSERT INTO group_role_rel VALUES (3, 4, 0)");
    statement.execute("INSERT INTO group_role_rel VALUES (3, 5, 0)");
    statement.execute("INSERT INTO group_role_rel VALUES (3, null, 0)");

    String sql =
        groupMetaH2Provider
            .listExtendedGroupPOsByMetalakeId(3L)
            .replace(QUERY_PARAM_METALAKE_ID, "3");
    try (ResultSet rs = statement.executeQuery(sql)) {
      rs.next();
      assertEquals("[\"role3\"]", rs.getString(COLUMN_LABEL_ROLE_NAMES));
      assertEquals("[\"3\",\"4\",\"5\"]", rs.getString(COLUMN_LABEL_ROLE_IDS));
    }
  }

  @AfterAll
  static void tearDown() throws SQLException {
    dropSchema(statement);
    statement.close();
    connection.close();
  }

  private static void dropSchema(Statement statement) throws SQLException {
    statement.execute("DROP TABLE IF EXISTS group_role_rel");
    statement.execute("DROP TABLE IF EXISTS role_meta");
    statement.execute("DROP TABLE IF EXISTS group_meta");
  }
}
