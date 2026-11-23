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
package org.apache.gravitino.storage.relational.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Verifies that table ID resolution never uses an aggregate column read. */
public class TestTableIdLookup {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMetadataOnlyReadAndMissingTable(boolean cacheEnabled) {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");
    TableMetaMapper mapper = mock(TableMetaMapper.class);
    TablePO table = mock(TablePO.class);
    when(table.getTableId()).thenReturn(42L);
    when(table.getSchemaId()).thenReturn(3L);
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.cacheEnabled()).thenReturn(cacheEnabled);
    when(env.config()).thenReturn(new Config(false) {});
    when(mapper.selectTableByFullQualifiedName("metalake", "catalog", "schema", "table"))
        .thenReturn(table);
    when(mapper.selectTableMetaBySchemaIdAndName(3L, "table")).thenReturn(table);
    try (MockedStatic<GravitinoEnv> mockedEnv = mockStatic(GravitinoEnv.class);
        MockedStatic<EntityIdService> ids = mockStatic(EntityIdService.class);
        MockedStatic<SessionUtils> sessions = mockStatic(SessionUtils.class)) {
      mockedEnv.when(GravitinoEnv::getInstance).thenReturn(env);
      ids.when(
              () ->
                  EntityIdService.getEntityId(
                      NameIdentifier.of("metalake", "catalog", "schema"), Entity.EntityType.SCHEMA))
          .thenReturn(3L);
      sessions
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<TableMetaMapper, TablePO> query = invocation.getArgument(1);
                return query.apply(mapper);
              });
      Assertions.assertEquals(42L, new JDBCBackend().getTableId(ident));
      if (cacheEnabled) {
        verify(mapper).selectTableMetaBySchemaIdAndName(3L, "table");
      } else {
        verify(mapper).selectTableByFullQualifiedName("metalake", "catalog", "schema", "table");
      }
      verifyNoMoreInteractions(mapper);
      when(mapper.selectTableMetaBySchemaIdAndName(3L, "table")).thenReturn(null);
      when(mapper.selectTableByFullQualifiedName("metalake", "catalog", "schema", "table"))
          .thenReturn(null);
      Assertions.assertThrows(
          NoSuchEntityException.class, () -> new JDBCBackend().getTableId(ident));
    }
  }
}
