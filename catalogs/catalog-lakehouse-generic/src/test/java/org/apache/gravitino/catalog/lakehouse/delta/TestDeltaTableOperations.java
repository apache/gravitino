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
package org.apache.gravitino.catalog.lakehouse.delta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDeltaTableOperations {

  @TempDir private java.nio.file.Path tempDir;

  private DeltaTableOperations deltaTableOps;
  private EntityStore store;
  private ManagedSchemaOperations schemaOps;
  private IdGenerator idGenerator;

  @BeforeEach
  public void setUp() {
    store = mock(EntityStore.class);
    schemaOps = mock(ManagedSchemaOperations.class);
    idGenerator = mock(IdGenerator.class);
    deltaTableOps = spy(new DeltaTableOperations(store, schemaOps, idGenerator));
  }

  @Test
  public void testCreateTableValidationFailures() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    String location = tempDir.resolve("delta_table").toString();

    // Test missing external property
    Map<String, String> noExternal = Maps.newHashMap();
    noExternal.put(Table.PROPERTY_LOCATION, location);
    IllegalArgumentException e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                deltaTableOps.createTable(
                    ident, columns, null, noExternal, new Transform[0], null, null, null));
    Assertions.assertTrue(e1.getMessage().contains("external Delta tables"));
    Assertions.assertTrue(e1.getMessage().contains("external=true"));

    // Test external=false
    Map<String, String> externalFalse = Maps.newHashMap();
    externalFalse.put(Table.PROPERTY_LOCATION, location);
    externalFalse.put(Table.PROPERTY_EXTERNAL, "false");
    IllegalArgumentException e2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                deltaTableOps.createTable(
                    ident, columns, null, externalFalse, new Transform[0], null, null, null));
    Assertions.assertTrue(e2.getMessage().contains("external Delta tables"));

    // Test missing location
    Map<String, String> noLocation = Maps.newHashMap();
    noLocation.put(Table.PROPERTY_EXTERNAL, "true");
    IllegalArgumentException e3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                deltaTableOps.createTable(
                    ident, columns, null, noLocation, new Transform[0], null, null, null));
    Assertions.assertTrue(e3.getMessage().contains("location"));
    Assertions.assertTrue(e3.getMessage().contains("required"));

    // Test blank location
    Map<String, String> blankLocation = Maps.newHashMap();
    blankLocation.put(Table.PROPERTY_EXTERNAL, "true");
    blankLocation.put(Table.PROPERTY_LOCATION, "  ");
    IllegalArgumentException e4 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                deltaTableOps.createTable(
                    ident, columns, null, blankLocation, new Transform[0], null, null, null));
    Assertions.assertTrue(e4.getMessage().contains("location"));

    // Test null properties
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            deltaTableOps.createTable(
                ident, columns, null, null, new Transform[0], null, null, null));
  }

  @Test
  public void testAlterTableThrowsException() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    TableChange[] changes =
        new TableChange[] {TableChange.addColumn(new String[] {"new_col"}, Types.StringType.get())};

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> deltaTableOps.alterTable(ident, changes));

    Assertions.assertTrue(exception.getMessage().contains("ALTER TABLE"));
    Assertions.assertTrue(exception.getMessage().contains("not supported"));
    Assertions.assertTrue(exception.getMessage().contains("Delta Lake APIs"));
  }

  @Test
  public void testPurgeTableThrowsException() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> deltaTableOps.purgeTable(ident));

    Assertions.assertTrue(exception.getMessage().contains("Purge"));
    Assertions.assertTrue(exception.getMessage().contains("not supported"));
    Assertions.assertTrue(exception.getMessage().contains("external"));
    Assertions.assertTrue(exception.getMessage().contains("dropTable()"));
  }
}
