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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestGroupRoleRelPostgreSQLProvider {

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void testBatchInsertGroupRoleRelOnDuplicateKeyUpdate(int batchSize) {
    List<GroupRoleRelPO> relations = new ArrayList<>();
    List<Object> expectedParameters = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      GroupRoleRelPO relation =
          GroupRoleRelPO.builder()
              .withGroupId(10L + i)
              .withRoleId(20L + i)
              .withAuditInfo("audit-" + i)
              .withCurrentVersion(30L + i)
              .withLastVersion(40L + i)
              .withDeletedAt(50L + i)
              .build();
      relations.add(relation);
      expectedParameters.addAll(
          Arrays.asList(
              relation.getGroupId(),
              relation.getRoleId(),
              relation.getAuditInfo(),
              relation.getCurrentVersion(),
              relation.getLastVersion(),
              relation.getDeletedAt()));
    }

    String script =
        new GroupRoleRelPostgreSQLProvider().batchInsertGroupRoleRelOnDuplicateKeyUpdate(relations);
    SqlSource sqlSource =
        new XMLLanguageDriver().createSqlSource(new Configuration(), script, Map.class);
    BoundSql boundSql = sqlSource.getBoundSql(Map.of("groupRoleRels", relations));
    String sql = boundSql.getSql().replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(sql.startsWith("INSERT INTO group_role_rel "));
    Assertions.assertTrue(
        sql.contains("ON CONFLICT (group_id, role_id, deleted_at) DO UPDATE SET"));
    Assertions.assertFalse(sql.contains("ON DUPLICATE KEY UPDATE"));
    Assertions.assertFalse(sql.contains("VALUES("));
    for (String column :
        Arrays.asList(
            "group_id", "role_id", "audit_info", "current_version", "last_version", "deleted_at")) {
      Assertions.assertTrue(sql.contains(column + " = EXCLUDED." + column));
    }

    List<Object> actualParameters = new ArrayList<>();
    boundSql
        .getParameterMappings()
        .forEach(
            mapping ->
                actualParameters.add(boundSql.getAdditionalParameter(mapping.getProperty())));
    Assertions.assertEquals(expectedParameters, actualParameters);
  }

  @Test
  void testSoftDeleteGroupRoleRelByGroupAndRolesWithEmptyRoles() {
    GroupRoleRelPostgreSQLProvider provider = new GroupRoleRelPostgreSQLProvider();
    String script = provider.softDeleteGroupRoleRelByGroupAndRoles(1L, Collections.emptyList());

    SqlSource sqlSource =
        new XMLLanguageDriver().createSqlSource(new Configuration(), script, Map.class);
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 1L);
    params.put("roleIds", Collections.emptyList());

    BoundSql boundSql = sqlSource.getBoundSql(params);
    String normalizedSql = boundSql.getSql().replaceAll("\\s+", " ").trim();

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Empty roleIds should not generate invalid SQL IN (...) with no values");

    Assertions.assertTrue(
        normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"),
        "Empty roleIds should result in an unsatisfiable WHERE clause (e.g., AND 1 = 0)");
  }

  @Test
  void testSoftDeleteGroupRoleRelByGroupAndRolesWithNonEmptyRoles() {
    GroupRoleRelPostgreSQLProvider provider = new GroupRoleRelPostgreSQLProvider();
    String script = provider.softDeleteGroupRoleRelByGroupAndRoles(1L, Arrays.asList(100L, 200L));

    SqlSource sqlSource =
        new XMLLanguageDriver().createSqlSource(new Configuration(), script, Map.class);
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 1L);
    params.put("roleIds", Arrays.asList(100L, 200L));

    BoundSql boundSql = sqlSource.getBoundSql(params);
    String normalizedSql = boundSql.getSql().replaceAll("\\s+", " ").trim();

    Assertions.assertTrue(
        normalizedSql.matches(".*\\brole_id\\s+IN\\s*\\(.*\\).*"),
        "Non-empty roleIds should generate SQL with role_id IN (...) clause");

    Assertions.assertFalse(
        normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"),
        "Non-empty roleIds should not contain unsatisfiable WHERE clause (1 = 0)");
  }

  @Test
  void testSoftDeleteGroupRoleRelByGroupAndRolesWithNullRoles() {
    GroupRoleRelPostgreSQLProvider provider = new GroupRoleRelPostgreSQLProvider();
    String script = provider.softDeleteGroupRoleRelByGroupAndRoles(1L, null);

    SqlSource sqlSource =
        new XMLLanguageDriver().createSqlSource(new Configuration(), script, Map.class);
    Map<String, Object> params = new HashMap<>();
    params.put("groupId", 1L);
    params.put("roleIds", null);

    BoundSql boundSql = sqlSource.getBoundSql(params);
    String normalizedSql = boundSql.getSql().replaceAll("\\s+", " ").trim();

    Assertions.assertFalse(
        normalizedSql.matches(".*\\bIN\\s*\\(\\s*\\).*"),
        "Null roleIds should not generate invalid SQL IN (...) with no values");

    Assertions.assertTrue(
        normalizedSql.matches(".*\\b1\\s*=\\s*0\\b.*"),
        "Null roleIds should result in an unsatisfiable WHERE clause (e.g., AND 1 = 0)");
  }
}
