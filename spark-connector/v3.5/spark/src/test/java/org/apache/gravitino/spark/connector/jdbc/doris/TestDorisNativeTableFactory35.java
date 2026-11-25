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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.doris.spark.catalog.DorisTable;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

/** Tests creation of an official Doris table from the validated schema and credential. */
public class TestDorisNativeTableFactory35 {
  @Test
  void testRequiresExactNativeColumnNamesAndTypes() {
    StructType schema = new StructType().add("id", DataTypes.IntegerType);
    assertTrue(DorisNativeTableFactory35.schemasCompatible(schema, schema));
    assertFalse(
        DorisNativeTableFactory35.schemasCompatible(
            schema, new StructType().add("ID", DataTypes.IntegerType)));
    assertFalse(
        DorisNativeTableFactory35.schemasCompatible(
            schema, new StructType().add("id", DataTypes.LongType)));
    assertFalse(DorisNativeTableFactory35.schemasCompatible(schema, new StructType()));
    StructType quoted = new StructType().add("a`b", DataTypes.IntegerType);
    assertFalse(DorisNativeTableFactory35.schemasCompatible(quoted, quoted));
    StructType dotted = new StructType().add("a.b", DataTypes.IntegerType);
    assertFalse(DorisNativeTableFactory35.schemasCompatible(dotted, dotted));
  }

  @Test
  @SuppressWarnings("deprecation")
  void testCreatesNativeTableAndClosesFrontend() throws Exception {
    Field column = mock(Field.class);
    when(column.getName()).thenReturn("id");
    when(column.getType()).thenReturn("INT");
    Schema physical = mock(Schema.class);
    when(physical.getProperties()).thenReturn(List.of(column));
    StructType logical = new StructType().add("id", DataTypes.IntegerType);
    try (MockedConstruction<DorisFrontendClient> frontends =
        mockConstruction(
            DorisFrontendClient.class,
            (frontend, context) ->
                when(frontend.getTableSchema("db", "table")).thenReturn(physical))) {
      Table table =
          DorisNativeTableFactory35.create(
              Identifier.of(new String[] {"db"}, "table"),
              logical,
              Map.of("doris.fenodes", "fe:8030"),
              new JdbcCredential("user", "secret"));
      assertTrue(table instanceof DorisTable);
      assertTrue(DorisNativeTableFactory35.schemasCompatible(logical, table.schema()));
      verify(frontends.constructed().get(0)).close();
    }
  }

  @Test
  void testInvalidConfigurationDoesNotExposeCredentials() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DorisNativeTableFactory35.create(
                    Identifier.of(new String[] {"db"}, "table"),
                    new StructType(),
                    Map.of("doris.fenodes", "invalid host:8030"),
                    new JdbcCredential("user", "test-secret")));
    assertTrue(!failure.toString().contains("test-secret"));
  }

  @Test
  void testFailedSchemaLoadClosesFrontendAndRedactsCause() throws Exception {
    try (MockedConstruction<DorisFrontendClient> frontends =
        mockConstruction(
            DorisFrontendClient.class,
            (frontend, context) ->
                when(frontend.getTableSchema("db", "table"))
                    .thenThrow(new IllegalStateException("test-secret")))) {
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DorisNativeTableFactory35.create(
                      Identifier.of(new String[] {"db"}, "table"),
                      new StructType(),
                      Map.of("doris.fenodes", "fe:8030"),
                      new JdbcCredential("user", "test-secret")));
      assertTrue(!failure.toString().contains("test-secret"));
      assertTrue(failure.getCause() == null);
      verify(frontends.constructed().get(0)).close();
    }
  }
}
