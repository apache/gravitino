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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGroupRoleRelBaseSQLProvider {

  @Test
  void testSoftDeleteGroupRoleRelByGroupAndRolesWithEmptyRoles() {
    GroupRoleRelBaseSQLProvider provider = new GroupRoleRelBaseSQLProvider();
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
}
