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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TestGlossaryTermMetaSQLProviderFactory {
  @ParameterizedTest
  @ValueSource(strings = {"h2", "mysql", "postgresql"})
  void testLockStatementForConfiguredBackend(String databaseId) {
    Configuration configuration = new Configuration();
    configuration.setDatabaseId(databaseId);
    configuration.addMapper(GlossaryTermMetaMapper.class);
    SqlSessionFactoryHelper helper = Mockito.mock(SqlSessionFactoryHelper.class);
    Mockito.when(helper.getSqlSessionFactory())
        .thenReturn(new SqlSessionFactoryBuilder().build(configuration));
    try (MockedStatic<SqlSessionFactoryHelper> helpers =
        Mockito.mockStatic(SqlSessionFactoryHelper.class)) {
      helpers.when(SqlSessionFactoryHelper::getInstance).thenReturn(helper);
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
  }
}
