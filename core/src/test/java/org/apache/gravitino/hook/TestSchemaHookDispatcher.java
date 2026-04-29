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
package org.apache.gravitino.hook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSchemaHookDispatcher {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SEPARATOR = ":";

  private SchemaDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  private SchemaHookDispatcher hookDispatcher;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    mockDispatcher = mock(SchemaDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);

    Config mockConfig = mock(Config.class);
    when(mockConfig.get(Configs.SCHEMA_SEPARATOR)).thenReturn(SEPARATOR);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", mockConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);

    hookDispatcher = new SchemaHookDispatcher(mockDispatcher);
    when(mockOwnerDispatcher.getOwner(any(), any())).thenReturn(Optional.empty());
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);
  }

  @Test
  public void testCreateFlatSchemaCreatesNoParents() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "myschema");
    Schema mockSchema = mock(Schema.class);
    when(mockDispatcher.createSchema(eq(ident), any(), any())).thenReturn(mockSchema);

    hookDispatcher.createSchema(ident, "comment", Collections.emptyMap());

    // Only the schema itself should be created, no parent creation calls
    verify(mockDispatcher, times(1)).createSchema(eq(ident), any(), any());
    verify(mockDispatcher, never()).schemaExists(any());

    // Owner should be set for the schema
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
  }

  @Test
  public void testCreateNestedSchemaAutoCreatesMissingParents() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B:C");
    NameIdentifier parentA = NameIdentifier.of(METALAKE, CATALOG, "A");
    NameIdentifier parentAB = NameIdentifier.of(METALAKE, CATALOG, "A:B");
    Schema mockSchema = mock(Schema.class);

    when(mockDispatcher.schemaExists(parentA)).thenReturn(false, true);
    when(mockDispatcher.schemaExists(parentAB)).thenReturn(false, true);
    when(mockDispatcher.createSchema(eq(ident), any(), any())).thenReturn(mockSchema);

    hookDispatcher.createSchema(ident, "comment", Collections.emptyMap());

    // Parents are auto-created by catalog; hook dispatcher only creates target schema.
    verify(mockDispatcher, never()).createSchema(eq(parentA), any(), any());
    verify(mockDispatcher, never()).createSchema(eq(parentAB), any(), any());
    verify(mockDispatcher, times(1)).createSchema(eq(ident), any(), any());

    // Verify owner is set for both auto-created parents and the requested schema.
    verify(mockOwnerDispatcher, times(3)).setOwner(eq(METALAKE), any(), any(), eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentA, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentAB, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
  }

  @Test
  public void testCreateNestedSchemaSkipsExistingParents() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B:C");
    NameIdentifier parentA = NameIdentifier.of(METALAKE, CATALOG, "A");
    NameIdentifier parentAB = NameIdentifier.of(METALAKE, CATALOG, "A:B");
    Schema mockSchema = mock(Schema.class);

    // A already exists, A:B does not
    when(mockDispatcher.schemaExists(parentA)).thenReturn(true);
    when(mockDispatcher.schemaExists(parentAB)).thenReturn(false, true);
    when(mockDispatcher.createSchema(eq(ident), any(), any())).thenReturn(mockSchema);

    hookDispatcher.createSchema(ident, "comment", Collections.emptyMap());

    // Parents are auto-created by catalog; hook dispatcher should not create them explicitly.
    verify(mockDispatcher, never()).createSchema(eq(parentA), any(), any());
    verify(mockDispatcher, never()).createSchema(eq(parentAB), any(), any());
    verify(mockDispatcher, times(1)).createSchema(eq(ident), any(), any());

    // Verify owner is set for the requested schema and the newly created parent A:B.
    verify(mockOwnerDispatcher, times(2)).setOwner(eq(METALAKE), any(), any(), eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, never())
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentA, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentAB, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
  }

  @Test
  public void testCreateNestedSchemaAllParentsExist() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B");
    NameIdentifier parentA = NameIdentifier.of(METALAKE, CATALOG, "A");
    Schema mockSchema = mock(Schema.class);

    when(mockDispatcher.schemaExists(parentA)).thenReturn(true);
    when(mockDispatcher.createSchema(eq(ident), any(), any())).thenReturn(mockSchema);

    hookDispatcher.createSchema(ident, "comment", Collections.emptyMap());

    // A should NOT be created since it exists
    verify(mockDispatcher, never()).createSchema(eq(parentA), isNull(), eq(Collections.emptyMap()));

    // Only A:B gets owner set
    verify(mockOwnerDispatcher, times(1)).setOwner(eq(METALAKE), any(), any(), eq(Owner.Type.USER));
  }

  @Test
  public void testCreateNestedSchemaDoesNotOverwriteOwnerOnConcurrentParentCreate() {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B:C");
    NameIdentifier parentA = NameIdentifier.of(METALAKE, CATALOG, "A");
    NameIdentifier parentAB = NameIdentifier.of(METALAKE, CATALOG, "A:B");
    Schema mockSchema = mock(Schema.class);

    when(mockDispatcher.schemaExists(parentA)).thenReturn(false, true);
    when(mockDispatcher.schemaExists(parentAB)).thenReturn(false, true);
    when(mockDispatcher.createSchema(eq(ident), any(), any())).thenReturn(mockSchema);
    when(mockOwnerDispatcher.getOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentA, Entity.EntityType.SCHEMA))))
        .thenReturn(Optional.of(mock(Owner.class)));
    when(mockOwnerDispatcher.getOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentAB, Entity.EntityType.SCHEMA))))
        .thenReturn(Optional.empty());

    hookDispatcher.createSchema(ident, "comment", Collections.emptyMap());

    verify(mockOwnerDispatcher, never())
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentA, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(parentAB, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, times(1))
        .setOwner(
            eq(METALAKE),
            eq(NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SCHEMA)),
            any(),
            eq(Owner.Type.USER));
  }

  @Test
  public void testCreateSchemaWithNullOwnerDispatcher() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);

    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, "A:B");
    NameIdentifier parentA = NameIdentifier.of(METALAKE, CATALOG, "A");
    Schema mockSchema = mock(Schema.class);

    when(mockDispatcher.schemaExists(parentA)).thenReturn(false);
    when(mockDispatcher.createSchema(eq(ident), any(), any())).thenReturn(mockSchema);

    // Should not throw even with null ownerDispatcher
    hookDispatcher.createSchema(ident, null, Collections.emptyMap());

    verify(mockDispatcher, never()).createSchema(eq(parentA), any(), any());
    verify(mockDispatcher, times(1)).createSchema(eq(ident), any(), any());
  }
}
