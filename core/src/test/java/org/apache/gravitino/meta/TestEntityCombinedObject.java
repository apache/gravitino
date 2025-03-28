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
package org.apache.gravitino.meta;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.EntityCombinedFileset;
import org.apache.gravitino.catalog.EntityCombinedModel;
import org.apache.gravitino.catalog.EntityCombinedSchema;
import org.apache.gravitino.catalog.EntityCombinedTable;
import org.apache.gravitino.catalog.EntityCombinedTopic;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEntityCombinedObject {
  private final AuditInfo auditInfo =
      AuditInfo.builder()
          .withCreator("creator")
          .withCreateTime(Instant.parse("2025-01-01T00:00:00Z"))
          .withLastModifier("modifier")
          .withLastModifiedTime(Instant.parse("2025-01-02T00:00:00Z"))
          .build();

  // Properties test data;
  private final Map<String, String> entityProperties =
      new HashMap<String, String>() {
        {
          put("k1", "v1");
          put("k2", "v2");
          put("k3", "k3");
          put("k5", null);
          put(null, null);
        }
      };
  private final Set<String> hiddenProperties = ImmutableSet.of("k3", "k4");
  private final Schema originSchema = mockSchema();
  private final Topic originTopic = mockTopic();
  private final Table originTable = mockTable();
  private final Fileset originFileset = mockFileset();
  private final Model originModel = mockModel();

  @Test
  public void testSchema() {
    EntityCombinedSchema entityCombinedSchema =
        EntityCombinedSchema.of(originSchema).withHiddenProperties(hiddenProperties);
    Assertions.assertEquals(originSchema.name(), entityCombinedSchema.name());
    Assertions.assertEquals(originSchema.comment(), entityCombinedSchema.comment());
    Map<String, String> filterProp = new HashMap<>(originSchema.properties());
    filterProp.remove("k3");
    filterProp.remove(null);
    filterProp.remove("k5");
    Assertions.assertEquals(filterProp, entityCombinedSchema.properties());
    Assertions.assertEquals(originSchema.auditInfo(), entityCombinedSchema.auditInfo());
  }

  @Test
  public void testTopic() {
    EntityCombinedTopic entityCombinedTopic =
        EntityCombinedTopic.of(originTopic).withHiddenProperties(hiddenProperties);
    Assertions.assertEquals(originTopic.name(), entityCombinedTopic.name());
    Assertions.assertEquals(originTopic.comment(), entityCombinedTopic.comment());
    Map<String, String> filterProp = new HashMap<>(originTopic.properties());
    filterProp.remove("k3");
    filterProp.remove(null);
    filterProp.remove("k5");
    Assertions.assertEquals(filterProp, entityCombinedTopic.properties());
    Assertions.assertEquals(originTopic.auditInfo(), entityCombinedTopic.auditInfo());
  }

  @Test
  public void testTable() {
    EntityCombinedTable entityCombinedTable =
        EntityCombinedTable.of(originTable).withHiddenProperties(hiddenProperties);
    Assertions.assertEquals(originTable.name(), entityCombinedTable.name());
    Assertions.assertEquals(originTable.comment(), entityCombinedTable.comment());
    Map<String, String> filterProp = new HashMap<>(originTopic.properties());
    filterProp.remove("k3");
    filterProp.remove(null);
    filterProp.remove("k5");
    Assertions.assertEquals(filterProp, entityCombinedTable.properties());
    Assertions.assertEquals(originTable.auditInfo(), entityCombinedTable.auditInfo());
  }

  @Test
  public void testFileset() {
    EntityCombinedFileset entityCombinedFileset =
        EntityCombinedFileset.of(originFileset).withHiddenProperties(hiddenProperties);
    Assertions.assertEquals(originFileset.name(), entityCombinedFileset.name());
    Assertions.assertEquals(originFileset.comment(), entityCombinedFileset.comment());
    Map<String, String> filterProp = new HashMap<>(originTopic.properties());
    filterProp.remove("k3");
    filterProp.remove(null);
    filterProp.remove("k5");
    Assertions.assertEquals(filterProp, entityCombinedFileset.properties());
    Assertions.assertEquals(originFileset.auditInfo(), entityCombinedFileset.auditInfo());
  }

  @Test
  public void testModel() {
    EntityCombinedModel entityCombinedModel =
        EntityCombinedModel.of(originModel).withHiddenProperties(hiddenProperties);
    Assertions.assertEquals(originModel.name(), entityCombinedModel.name());
    Assertions.assertEquals(originModel.comment(), entityCombinedModel.comment());
    Map<String, String> filterProp = new HashMap<>(originModel.properties());
    filterProp.remove("k3");
    filterProp.remove(null);
    filterProp.remove("k5");
    Assertions.assertEquals(filterProp, entityCombinedModel.properties());
    Assertions.assertEquals(originModel.auditInfo(), entityCombinedModel.auditInfo());
  }

  private Schema mockSchema() {
    Schema mockSchema = mock(Schema.class);
    Mockito.when(mockSchema.name()).thenReturn("testSchema");
    Mockito.when(mockSchema.comment()).thenReturn("test schema comment");
    Mockito.when(mockSchema.auditInfo()).thenReturn(auditInfo);
    Mockito.when(mockSchema.properties()).thenReturn(entityProperties);
    return mockSchema;
  }

  private Topic mockTopic() {
    Topic mockTopic = mock(Topic.class);
    Mockito.when(mockTopic.name()).thenReturn("testTopic");
    Mockito.when(mockTopic.comment()).thenReturn("test topic comment");
    Mockito.when(mockTopic.auditInfo()).thenReturn(auditInfo);
    Mockito.when(mockTopic.properties()).thenReturn(entityProperties);
    return mockTopic;
  }

  private Fileset mockFileset() {
    Fileset mockFileset = mock(Fileset.class);
    Mockito.when(mockFileset.name()).thenReturn("testFileset");
    Mockito.when(mockFileset.comment()).thenReturn("test fileset comment");
    Mockito.when(mockFileset.auditInfo()).thenReturn(auditInfo);
    Mockito.when(mockFileset.properties()).thenReturn(entityProperties);
    return mockFileset;
  }

  private Table mockTable() {
    Table mockTable = mock(Table.class);
    Mockito.when(mockTable.name()).thenReturn("testTable");
    Mockito.when(mockTable.comment()).thenReturn("test table comment");
    Mockito.when(mockTable.auditInfo()).thenReturn(auditInfo);
    Mockito.when(mockTable.properties()).thenReturn(entityProperties);
    return mockTable;
  }

  private Model mockModel() {
    Model mockModel = mock(Model.class);
    Mockito.when(mockModel.name()).thenReturn("testModel");
    Mockito.when(mockModel.comment()).thenReturn("test model comment");
    Mockito.when(mockModel.auditInfo()).thenReturn(auditInfo);
    Mockito.when(mockModel.properties()).thenReturn(entityProperties);
    return mockModel;
  }
}
