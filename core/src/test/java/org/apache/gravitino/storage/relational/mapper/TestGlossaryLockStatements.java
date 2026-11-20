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
package org.apache.gravitino.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TestGlossaryLockStatements {
  private final Configuration configuration = new Configuration();
  private MockedStatic<SqlSessionFactoryHelper> helpers;

  @BeforeEach
  void setUp() {
    configuration.addMapper(GlossaryTermMetaMapper.class);
    configuration.addMapper(GlossaryTermRelMapper.class);
    SqlSessionFactoryHelper helper = Mockito.mock(SqlSessionFactoryHelper.class);
    Mockito.when(helper.getSqlSessionFactory())
        .thenReturn(new SqlSessionFactoryBuilder().build(configuration));
    helpers = Mockito.mockStatic(SqlSessionFactoryHelper.class);
    helpers.when(SqlSessionFactoryHelper::getInstance).thenReturn(helper);
  }

  @AfterEach
  void tearDown() {
    helpers.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"h2", "mysql", "postgresql"})
  void testLockStatementForConfiguredBackend(String databaseId) {
    configuration.setDatabaseId(databaseId);
    BoundSql statement =
        configuration
            .getMappedStatement(GlossaryTermMetaMapper.class.getName() + ".getForUpdate")
            .getBoundSql(Map.of("metalakeId", 7L, "key", "cost.net"));
    boolean mysql = databaseId.equals("mysql");
    assertEquals(mysql, statement.getSql().contains("FORCE INDEX (PRIMARY)"));
    assertTrue(statement.getSql().endsWith("FOR UPDATE"));
    assertEquals(
        mysql ? List.of("metalakeId", "key", "metalakeId", "key") : List.of("metalakeId", "key"),
        statement.getParameterMappings().stream().map(ParameterMapping::getProperty).toList());
  }

  @ParameterizedTest
  @CsvSource({
    "h2, MERGE INTO",
    "mysql, ON DUPLICATE KEY UPDATE",
    "postgresql, ON CONFLICT (metalake_id) DO NOTHING"
  })
  void testEnsureHierarchyLockBindsMetalakeId(String databaseId, String dialectClause)
      throws NoSuchMethodException, SQLException {
    configuration.setDatabaseId(databaseId);
    long metalakeId = 73L;
    Object params =
        new ParamNameResolver(
                configuration, GlossaryTermRelMapper.class.getMethod("ensureLock", long.class))
            .getNamedParams(new Object[] {metalakeId});
    MappedStatement statement =
        configuration.getMappedStatement(GlossaryTermRelMapper.class.getName() + ".ensureLock");
    BoundSql sql = statement.getBoundSql(params);
    assertTrue(sql.getSql().contains(dialectClause));
    PreparedStatement prepared = Mockito.mock(PreparedStatement.class);
    configuration.newParameterHandler(statement, params, sql).setParameters(prepared);
    Mockito.verify(prepared).setLong(1, metalakeId);
  }
}
