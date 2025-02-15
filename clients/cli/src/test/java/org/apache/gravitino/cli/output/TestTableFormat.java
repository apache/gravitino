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

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.outputs.Column;
import org.apache.gravitino.cli.outputs.TableFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestTableFormat {
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
  void testCreateDefaultTableFormat() {
    CommandContext mockContext = getMockContext();

    Column columnA = new Column(mockContext, "METALAKE");
    Column columnB = new Column(mockContext, "COMMENT");

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------+---------+\n"
            + "| METALAKE | COMMENT |\n"
            + "+----------+---------+\n"
            + "| cell1    | cell4   |\n"
            + "| cell2    | cell5   |\n"
            + "| cell3    | cell6   |\n"
            + "+----------+---------+",
        outputString);
  }

  @Test
  void testTitleWithLeftAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "METALAKE", Column.HorizontalAlign.LEFT, Column.HorizontalAlign.CENTER);
    Column columnB =
        new Column(
            mockContext, "COMMENT", Column.HorizontalAlign.LEFT, Column.HorizontalAlign.CENTER);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "| METALAKE       | COMMENT        |\n"
            + "+----------------+----------------+\n"
            + "|     cell1      |     cell4      |\n"
            + "|     cell2      |     cell5      |\n"
            + "|     cell3      |     cell6      |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testTitleWithRightAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "METALAKE", Column.HorizontalAlign.RIGHT, Column.HorizontalAlign.CENTER);
    Column columnB =
        new Column(
            mockContext, "COMMENT", Column.HorizontalAlign.RIGHT, Column.HorizontalAlign.CENTER);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "|       METALAKE |        COMMENT |\n"
            + "+----------------+----------------+\n"
            + "|     cell1      |     cell4      |\n"
            + "|     cell2      |     cell5      |\n"
            + "|     cell3      |     cell6      |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testDataWithCenterAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "METALAKE", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.CENTER);
    Column columnB =
        new Column(
            mockContext, "COMMENT", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.CENTER);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "|    METALAKE    |    COMMENT     |\n"
            + "+----------------+----------------+\n"
            + "|     cell1      |     cell4      |\n"
            + "|     cell2      |     cell5      |\n"
            + "|     cell3      |     cell6      |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testDataWithRightAlign() {
    CommandContext mockContext = getMockContext();

    Column columnA =
        new Column(
            mockContext, "METALAKE", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.RIGHT);
    Column columnB =
        new Column(
            mockContext, "COMMENT", Column.HorizontalAlign.CENTER, Column.HorizontalAlign.RIGHT);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+----------------+----------------+\n"
            + "|    METALAKE    |    COMMENT     |\n"
            + "+----------------+----------------+\n"
            + "|          cell1 |          cell4 |\n"
            + "|          cell2 |          cell5 |\n"
            + "|          cell3 |          cell6 |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testTableOutputWithLimit() {
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.outputLimit()).thenReturn(5);

    Column columnA = new Column(mockContext, "METALAKE");
    Column columnB = new Column(mockContext, "comment");

    addRepeatedCells(columnA, 10);
    addRepeatedCells(columnB, 10);

    TableFormat<String> tableFormat =
        new TableFormat<String>(mockContext) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+------------+-----------+\n"
            + "|  METALAKE  |  comment  |\n"
            + "+------------+-----------+\n"
            + "| METALAKE-1 | comment-1 |\n"
            + "| METALAKE-2 | comment-2 |\n"
            + "| METALAKE-3 | comment-3 |\n"
            + "| METALAKE-4 | comment-4 |\n"
            + "| METALAKE-5 | comment-5 |\n"
            + "| …          | …         |\n"
            + "+------------+-----------+",
        outputString);
  }

  @Test
  void testMetalakeDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();

    Metalake mockMetalake = getMockMetalake();
    TableFormat.output(mockMetalake, mockContext);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---------------+-------------------------+\n"
            + "|   METALAKE    |         COMMENT         |\n"
            + "+---------------+-------------------------+\n"
            + "| demo_metalake | This is a demo metalake |\n"
            + "+---------------+-------------------------+",
        output);
  }

  @Test
  void testListMetalakeWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Metalake mockMetalake1 = getMockMetalake("metalake1", "This is a metalake");
    Metalake mockMetalake2 = getMockMetalake("metalake2", "This is another metalake");

    TableFormat.output(new Metalake[] {mockMetalake1, mockMetalake2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----------+\n"
            + "| METALAKE  |\n"
            + "+-----------+\n"
            + "| metalake1 |\n"
            + "| metalake2 |\n"
            + "+-----------+",
        output);
  }

  @Test
  void testCatalogDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog = getMockCatalog();

    TableFormat.output(mockCatalog, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+--------------+------------+---------------+------------------------+\n"
            + "|   CATALOG    |    TYPE    |   PROVIDER    |        COMMENT         |\n"
            + "+--------------+------------+---------------+------------------------+\n"
            + "| demo_catalog | RELATIONAL | demo_provider | This is a demo catalog |\n"
            + "+--------------+------------+---------------+------------------------+",
        output);
  }

  @Test
  void testListCatalogWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog1 =
        getMockCatalog("catalog1", Catalog.Type.FILESET, "provider1", "This is a catalog");
    Catalog mockCatalog2 =
        getMockCatalog("catalog2", Catalog.Type.RELATIONAL, "provider2", "This is another catalog");

    TableFormat.output(new Catalog[] {mockCatalog1, mockCatalog2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+----------+\n"
            + "| CATALOG  |\n"
            + "+----------+\n"
            + "| catalog1 |\n"
            + "| catalog2 |\n"
            + "+----------+",
        output);
  }

  @Test
  void testSchemaDetailsWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Schema mockSchema = getMockSchema();
    TableFormat.output(mockSchema, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-------------+-----------------------+\n"
            + "|   SCHEMA    |        COMMENT        |\n"
            + "+-------------+-----------------------+\n"
            + "| demo_schema | This is a demo schema |\n"
            + "+-------------+-----------------------+",
        output);
  }

  @Test
  void testListSchemaWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Schema mockSchema1 = getMockSchema("schema1", "This is a schema");
    Schema mockSchema2 = getMockSchema("schema2", "This is another schema");

    TableFormat.output(new Schema[] {mockSchema1, mockSchema2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---------+\n"
            + "| SCHEMA  |\n"
            + "+---------+\n"
            + "| schema1 |\n"
            + "| schema2 |\n"
            + "+---------+",
        output);
  }

  @Test
  void testAuditWithTableFormat() {
    CommandContext mockContext = getMockContext();
    Audit mockAudit = getMockAudit();
    TableFormat.output(mockAudit, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-----------+--------------------------+-----------+--------------------------+\n"
            + "|  creator  |       create_time        | modified  |       modify_time        |\n"
            + "+-----------+--------------------------+-----------+--------------------------+\n"
            + "| demo_user | 2021-01-20T02:51:51.111Z | demo_user | 2021-01-20T02:51:51.111Z |\n"
            + "+-----------+--------------------------+-----------+--------------------------+",
        output);
  }

  @Test
  void testOutputWithUnsupportType() {
    CommandContext mockContext = getMockContext();
    Object mockObject = new Object();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          TableFormat.output(mockObject, mockContext);
        });
  }

  private void addRepeatedCells(Column column, int count) {
    for (int i = 0; i < count; i++) {
      column.addCell(column.getHeader() + "-" + (i + 1));
    }
  }

  private CommandContext getMockContext() {
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.outputLimit()).thenReturn(-1);

    return mockContext;
  }

  private Metalake getMockMetalake() {
    return getMockMetalake("demo_metalake", "This is a demo metalake");
  }

  private Metalake getMockMetalake(String name, String comment) {
    Metalake mockMetalake = mock(Metalake.class);
    when(mockMetalake.name()).thenReturn(name);
    when(mockMetalake.comment()).thenReturn(comment);

    return mockMetalake;
  }

  private Catalog getMockCatalog() {
    return getMockCatalog(
        "demo_catalog", Catalog.Type.RELATIONAL, "demo_provider", "This is a demo catalog");
  }

  private Catalog getMockCatalog(String name, Catalog.Type type, String provider, String comment) {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn(name);
    when(mockCatalog.type()).thenReturn(type);
    when(mockCatalog.provider()).thenReturn(provider);
    when(mockCatalog.comment()).thenReturn(comment);

    return mockCatalog;
  }

  private Schema getMockSchema() {
    return getMockSchema("demo_schema", "This is a demo schema");
  }

  private Schema getMockSchema(String name, String comment) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);

    return mockSchema;
  }

  private Audit getMockAudit() {
    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(mockAudit.lastModifier()).thenReturn("demo_user");
    when(mockAudit.lastModifiedTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));

    return mockAudit;
  }
}
