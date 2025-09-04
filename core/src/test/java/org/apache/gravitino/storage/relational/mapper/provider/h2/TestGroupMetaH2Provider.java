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
import java.sql.Statement;
import org.junit.jupiter.api.Test;

public class TestGroupMetaH2Provider {

  @Test
  public void testListExtendedGroupPOsByMetalakeIdWithoutRoles() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=MYSQL", "sa", "");
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE group_meta (group_id BIGINT, group_name VARCHAR(255), metalake_id BIGINT, "
              + "audit_info VARCHAR(255), current_version BIGINT, last_version BIGINT, deleted_at BIGINT)");
      stmt.execute(
          "CREATE TABLE group_role_rel (group_id BIGINT, role_id BIGINT, deleted_at BIGINT)");
      stmt.execute(
          "CREATE TABLE role_meta (role_id BIGINT, role_name VARCHAR(255), deleted_at BIGINT)");

      stmt.execute("INSERT INTO group_meta VALUES (1, 'g1', 1, 'audit', 0, 0, 0)");

      GroupMetaH2Provider provider = new GroupMetaH2Provider();
      String sql = provider.listExtendedGroupPOsByMetalakeId(1L).replace("#{metalakeId}", "1");
      try (ResultSet rs = stmt.executeQuery(sql)) {
        rs.next();
        assertEquals("[]", rs.getString("roleNames"));
        assertEquals("[]", rs.getString("roleIds"));
      }
    }
  }

  @Test
  public void testListExtendedGroupPOsByMetalakeIdWithRoles() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=MYSQL", "sa", "");
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE group_meta (group_id BIGINT, group_name VARCHAR(255), metalake_id BIGINT, "
              + "audit_info VARCHAR(255), current_version BIGINT, last_version BIGINT, deleted_at BIGINT)");
      stmt.execute(
          "CREATE TABLE group_role_rel (group_id BIGINT, role_id BIGINT, deleted_at BIGINT)");
      stmt.execute(
          "CREATE TABLE role_meta (role_id BIGINT, role_name VARCHAR(255), deleted_at BIGINT)");

      stmt.execute("INSERT INTO group_meta VALUES (2, 'g2', 1, 'audit2', 0, 0, 0)");
      stmt.execute("INSERT INTO role_meta VALUES (1, 'foo', 0)");
      stmt.execute("INSERT INTO role_meta VALUES (2, 'bar', 0)");
      stmt.execute("INSERT INTO group_role_rel VALUES (2, 1, 0)");
      stmt.execute("INSERT INTO group_role_rel VALUES (2, 2, 0)");

      GroupMetaH2Provider provider = new GroupMetaH2Provider();
      String sql = provider.listExtendedGroupPOsByMetalakeId(1L).replace("#{metalakeId}", "1");
      try (ResultSet rs = stmt.executeQuery(sql)) {
        rs.next();
        assertEquals("[\"foo\",\"bar\"]", rs.getString("roleNames"));
        assertEquals("[\"1\",\"2\"]", rs.getString("roleIds"));
      }
    }
  }
}
