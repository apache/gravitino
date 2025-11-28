/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.lance;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_CREATION_MODE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLanceTableOperations {

  @TempDir private java.nio.file.Path tempDir;

  private LanceTableOperations lanceTableOps;
  private EntityStore store;
  private ManagedSchemaOperations schemaOps;
  private IdGenerator idGenerator;

  @BeforeEach
  public void setUp() {
    store = mock(EntityStore.class);
    schemaOps = mock(ManagedSchemaOperations.class);
    idGenerator = mock(IdGenerator.class);
    lanceTableOps = spy(new LanceTableOperations(store, schemaOps, idGenerator));
  }

  @Test
  public void testCreationModeEnum() {
    // Test that CreationMode enum has expected values
    Assertions.assertEquals(3, LanceTableOperations.CreationMode.values().length);
    Assertions.assertNotNull(LanceTableOperations.CreationMode.valueOf("CREATE"));
    Assertions.assertNotNull(LanceTableOperations.CreationMode.valueOf("EXIST_OK"));
    Assertions.assertNotNull(LanceTableOperations.CreationMode.valueOf("OVERWRITE"));
  }

  @Test
  public void testCreateTableWithInvalidMode() {
    // Arrange
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    String location = tempDir.resolve("table6").toString();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(LANCE_CREATION_MODE, "INVALID_MODE");

    // Act & Assert
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            lanceTableOps.createTable(
                ident,
                columns,
                null,
                properties,
                new Transform[0],
                null,
                new SortOrder[0],
                new Index[0]));
  }
}
