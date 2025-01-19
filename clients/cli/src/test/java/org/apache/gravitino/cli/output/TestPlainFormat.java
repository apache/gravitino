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

package org.apache.gravitino.cli.output;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.outputs.OutputProperty;
import org.apache.gravitino.cli.outputs.PlainFormat;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.base.Joiner;

public class TestPlainFormat {
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();
  public static final Joiner NEWLINE_JOINER = Joiner.on(System.lineSeparator()).skipNulls();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setUp() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testUnsupportType() {
    Fileset mockFileset = mock(Fileset.class);
    OutputProperty property = OutputProperty.defaultOutputProperty();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PlainFormat.output(mockFileset, property));
  }

  @Test
  void testMetalakePlainFormat() {
    Metalake mockMetalake = mock(Metalake.class);
    when(mockMetalake.name()).thenReturn("demo_metalake");
    when(mockMetalake.comment()).thenReturn("metalake comment");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(mockMetalake, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        COMMA_JOINER.join(Arrays.asList("demo_metalake", "metalake comment")), output);
  }

  @Test
  void testMetalakesPlainFormat() {
    Metalake mockMetalake1 = mock(Metalake.class);
    Metalake mockMetalake2 = mock(Metalake.class);

    when(mockMetalake1.name()).thenReturn("demo_metalake1");
    when(mockMetalake2.name()).thenReturn("demo_metalake2");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(new Metalake[] {mockMetalake1, mockMetalake2}, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        NEWLINE_JOINER.join(Arrays.asList("demo_metalake1", "demo_metalake2")), output);
  }

  @Test
  void testCatalogPlainFormat() {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn("demo_catalog");
    when(mockCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(mockCatalog.provider()).thenReturn("demo_provider");
    when(mockCatalog.comment()).thenReturn("catalog comment");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(mockCatalog, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        COMMA_JOINER.join(
            Arrays.asList("demo_catalog", "RELATIONAL", "demo_provider", "catalog comment")),
        output);
  }

  @Test
  void testCatalogsPlainFormat() {
    Catalog mockCatalog1 = mock(Catalog.class);
    Catalog mockCatalog2 = mock(Catalog.class);

    when(mockCatalog1.name()).thenReturn("demo_catalog1");
    when(mockCatalog2.name()).thenReturn("demo_catalog2");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(new Catalog[] {mockCatalog1, mockCatalog2}, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        NEWLINE_JOINER.join(Arrays.asList("demo_catalog1", "demo_catalog2")), output);
  }

  @Test
  void testSchemaPlainFormat() {
    Schema mmockSchema = mock(Schema.class);
    when(mmockSchema.name()).thenReturn("demo_schema");
    when(mmockSchema.comment()).thenReturn("schema comment");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(mmockSchema, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        COMMA_JOINER.join(Arrays.asList("demo_schema", "schema comment")), output);
  }

  @Test
  void testSchemasPlainFormat() {
    Schema mockSchema1 = mock(Schema.class);
    Schema mockSchema2 = mock(Schema.class);

    when(mockSchema1.name()).thenReturn("demo_schema1");
    when(mockSchema2.name()).thenReturn("demo_schema2");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(new Schema[] {mockSchema1, mockSchema2}, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        NEWLINE_JOINER.join(Arrays.asList("demo_schema1", "demo_schema2")), output);
  }

  @Test
  void testTablePlainFormat() {
    Table mockTable = mock(Table.class);
    Column mockColumn1 = mock(Column.class);
    Column mockColumn2 = mock(Column.class);
    Type mockType1 = mock(Type.class);
    Type mockType2 = mock(Type.class);
    when(mockType1.simpleString()).thenReturn("int");
    when(mockType2.simpleString()).thenReturn("string");
    when(mockColumn1.name()).thenReturn("demo_column1");
    when(mockColumn2.name()).thenReturn("demo_column2");
    when(mockColumn1.dataType()).thenReturn(mockType1);
    when(mockColumn2.dataType()).thenReturn(mockType2);
    when(mockColumn1.comment()).thenReturn("column1 comment");
    when(mockColumn2.comment()).thenReturn("column2 comment");

    when(mockTable.name()).thenReturn("demo_table");
    when(mockTable.columns()).thenReturn(new Column[] {mockColumn1, mockColumn2});
    when(mockTable.comment()).thenReturn("table comment");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(mockTable, property);

    StringBuilder sb = new StringBuilder();
    sb.append("demo_table\n");

    sb.append(
        NEWLINE_JOINER.join(
            Arrays.asList(
                COMMA_JOINER.join(Arrays.asList("demo_column1", "int", "column1 comment")),
                COMMA_JOINER.join(Arrays.asList("demo_column2", "string", "column2 comment")))));
    sb.append("\n");
    sb.append("table comment");
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(sb.toString(), output);
  }

  @Test
  void testTablesPlainFormat() {
    Table mockSchema1 = mock(Table.class);
    Table mockSchema2 = mock(Table.class);

    when(mockSchema1.name()).thenReturn("demo_table1");
    when(mockSchema2.name()).thenReturn("demo_table2");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    PlainFormat.output(new Table[] {mockSchema1, mockSchema2}, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        NEWLINE_JOINER.join(Arrays.asList("demo_table1", "demo_table2")), output);
  }
}
