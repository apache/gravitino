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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.outputs.BorderStyle;
import org.apache.gravitino.cli.outputs.Column;
import org.apache.gravitino.cli.outputs.HorizontalAlign;
import org.apache.gravitino.cli.outputs.OutputProperty;
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
  void testTableOutputWithFooter() {

    Column columnA = new Column("METALAKE", "FooterA", OutputProperty.defaultOutputProperty());
    Column columnB = new Column("comment", "FooterB", OutputProperty.defaultOutputProperty());

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(OutputProperty.defaultOutputProperty()) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+---+----------+---------+\n"
            + "|   | METALAKE | COMMENT |\n"
            + "+---+----------+---------+\n"
            + "| 1 | cell1    | cell4   |\n"
            + "| 2 | cell2    | cell5   |\n"
            + "| 3 | cell3    | cell6   |\n"
            + "+---+----------+---------+\n"
            + "|   | FooterA  | FooterB |\n"
            + "+---+----------+---------+",
        outputString);
  }

  @Test
  void testTableOutputWithoutFooter() {
    Column columnA = new Column("METALAKE", null, OutputProperty.defaultOutputProperty());
    Column columnB = new Column("comment", null, OutputProperty.defaultOutputProperty());

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(OutputProperty.defaultOutputProperty()) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+---+----------+---------+\n"
            + "|   | METALAKE | COMMENT |\n"
            + "+---+----------+---------+\n"
            + "| 1 | cell1    | cell4   |\n"
            + "| 2 | cell2    | cell5   |\n"
            + "| 3 | cell3    | cell6   |\n"
            + "+---+----------+---------+",
        outputString);
  }

  @Test
  void testTableOutputWithoutRowNumber() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setRowNumbersEnabled(false);
    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
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
  void testTableOutputWithFancyStyle() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.FANCY);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "╔═══╤══════════╤═════════╗\n"
            + "║   │ METALAKE │ COMMENT ║\n"
            + "╠═══╪══════════╪═════════╣\n"
            + "║ 1 │ cell1    │ cell4   ║\n"
            + "║ 2 │ cell2    │ cell5   ║\n"
            + "║ 3 │ cell3    │ cell6   ║\n"
            + "╚═══╧══════════╧═════════╝",
        outputString);
  }

  @Test
  void testTableOutputWithFancy2Style() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.FANCY2);
    property.setRowNumbersEnabled(false);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "╔══════════╤═════════╗\n"
            + "║ METALAKE │ COMMENT ║\n"
            + "╠══════════╪═════════╣\n"
            + "║ cell1    │ cell4   ║\n"
            + "╟──────────┼─────────╢\n"
            + "║ cell2    │ cell5   ║\n"
            + "╟──────────┼─────────╢\n"
            + "║ cell3    │ cell6   ║\n"
            + "╚══════════╧═════════╝",
        outputString);
  }

  @Test
  void testTableOutputWithBasic2Style() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.BASIC2);
    property.setRowNumbersEnabled(false);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
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
  void testTableOutputWithLimit() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.BASIC2);
    property.setLimit(5);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    addRepeatedCells(columnA, 10);
    addRepeatedCells(columnB, 10);

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();
    Assertions.assertEquals(
        "+---+------------+-----------+\n"
            + "|   |  METALAKE  |  COMMENT  |\n"
            + "+---+------------+-----------+\n"
            + "| 1 | METALAKE-1 | COMMENT-1 |\n"
            + "| 2 | METALAKE-2 | COMMENT-2 |\n"
            + "| 3 | METALAKE-3 | COMMENT-3 |\n"
            + "| 4 | METALAKE-4 | COMMENT-4 |\n"
            + "| 5 | METALAKE-5 | COMMENT-5 |\n"
            + "| 6 | …          | …         |\n"
            + "+---+------------+-----------+",
        outputString);
  }

  @Test
  void testTableOutputWithHeaderLeftAlignment() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.BASIC2);
    property.setRowNumbersEnabled(false);
    property.setHeaderAlign(HorizontalAlign.LEFT);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
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
            + "| cell1          | cell4          |\n"
            + "| cell2          | cell5          |\n"
            + "| cell3          | cell6          |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testTableOutputWithHeaderRightAlignment() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.BASIC2);
    property.setRowNumbersEnabled(false);
    property.setHeaderAlign(HorizontalAlign.RIGHT);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
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
            + "| cell1          | cell4          |\n"
            + "| cell2          | cell5          |\n"
            + "| cell3          | cell6          |\n"
            + "| very long cell | very long cell |\n"
            + "+----------------+----------------+",
        outputString);
  }

  @Test
  void testTableOutputWithDataCenterAlignment() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.BASIC2);
    property.setRowNumbersEnabled(false);
    property.setDataAlign(HorizontalAlign.CENTER);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
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
  void testTableOutputWithDataRightAlignment() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.BASIC2);
    property.setRowNumbersEnabled(false);
    property.setDataAlign(HorizontalAlign.RIGHT);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    columnA.addCell("cell1").addCell("cell2").addCell("cell3").addCell("very long cell");
    columnB.addCell("cell4").addCell("cell5").addCell("cell6").addCell("very long cell");

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
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
  void testTableOutputWithEmptyColumn() {
    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setStyle(BorderStyle.FANCY);
    property.setRowNumbersEnabled(false);

    Column columnA = new Column("METALAKE", null, property);
    Column columnB = new Column("comment", null, property);

    TableFormat<String> tableFormat =
        new TableFormat<String>(property) {
          @Override
          public String getOutput(String entity) {
            return null;
          }
        };

    String outputString = tableFormat.getTableFormat(columnA, columnB).trim();

    Assertions.assertEquals(
        "╔══════════╤═════════╗\n"
            + "║ METALAKE │ COMMENT ║\n"
            + "╠══════════╪═════════╣\n"
            + "╚══════════╧═════════╝",
        outputString);
  }

  @Test
  void testMetalakeTableOutput() {
    Metalake mockMetalake = mock(Metalake.class);
    when(mockMetalake.name()).thenReturn("demo_metalake");
    when(mockMetalake.comment()).thenReturn("metalake comment");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setRowNumbersEnabled(false);
    TableFormat.output(mockMetalake, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---------------+------------------+\n"
            + "|   METALAKE    |     COMMENT      |\n"
            + "+---------------+------------------+\n"
            + "| demo_metalake | metalake comment |\n"
            + "+---------------+------------------+",
        output);
  }

  @Test
  void testMetalakesTableOutput() {
    Metalake mockMetalake1 = mock(Metalake.class);
    Metalake mockMetalake2 = mock(Metalake.class);

    when(mockMetalake1.name()).thenReturn("demo_metalake1");
    when(mockMetalake2.name()).thenReturn("demo_metalake2");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setRowNumbersEnabled(false);
    TableFormat.output(new Metalake[] {mockMetalake1, mockMetalake2}, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+----------------+\n"
            + "|    METALAKE    |\n"
            + "+----------------+\n"
            + "| demo_metalake1 |\n"
            + "| demo_metalake2 |\n"
            + "+----------------+",
        output);
  }

  @Test
  void testCatalogTableOutput() {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn("demo_catalog");
    when(mockCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(mockCatalog.provider()).thenReturn("demo_provider");
    when(mockCatalog.comment()).thenReturn("catalog comment");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    TableFormat.output(mockCatalog, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---+--------------+------------+---------------+-----------------+\n"
            + "|   |   CATALOG    |    TYPE    |   PROVIDER    |     COMMENT     |\n"
            + "+---+--------------+------------+---------------+-----------------+\n"
            + "| 1 | demo_catalog | RELATIONAL | demo_provider | catalog comment |\n"
            + "+---+--------------+------------+---------------+-----------------+",
        output);
  }

  @Test
  void testCatalogsTableOutput() {
    Catalog mockCatalog1 = mock(Catalog.class);
    Catalog mockCatalog2 = mock(Catalog.class);

    when(mockCatalog1.name()).thenReturn("demo_catalog1");
    when(mockCatalog2.name()).thenReturn("demo_catalog2");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setRowNumbersEnabled(false);
    TableFormat.output(new Catalog[] {mockCatalog1, mockCatalog2}, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+---------------+\n"
            + "|   METALAKE    |\n"
            + "+---------------+\n"
            + "| demo_catalog1 |\n"
            + "| demo_catalog2 |\n"
            + "+---------------+",
        output);
  }

  @Test
  void testSchemaTableOutput() {
    Schema mmockSchema = mock(Schema.class);
    when(mmockSchema.name()).thenReturn("demo_schema");
    when(mmockSchema.comment()).thenReturn("schema comment");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setRowNumbersEnabled(false);
    TableFormat.output(mmockSchema, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+-------------+----------------+\n"
            + "|   SCHEMA    |    COMMENT     |\n"
            + "+-------------+----------------+\n"
            + "| demo_schema | schema comment |\n"
            + "+-------------+----------------+",
        output);
  }

  @Test
  void testSchemasTableOutput() {
    Schema mockSchema1 = mock(Schema.class);
    Schema mockSchema2 = mock(Schema.class);

    when(mockSchema1.name()).thenReturn("demo_schema1");
    when(mockSchema2.name()).thenReturn("demo_schema2");

    OutputProperty property = OutputProperty.defaultOutputProperty();
    property.setRowNumbersEnabled(false);
    TableFormat.output(new Schema[] {mockSchema1, mockSchema2}, property);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "+--------------+\n"
            + "|    SCHEMA    |\n"
            + "+--------------+\n"
            + "| demo_schema1 |\n"
            + "| demo_schema2 |\n"
            + "+--------------+",
        output);
  }

  private void addRepeatedCells(Column column, int count) {
    for (int i = 0; i < count; i++) {
      column.addCell(column.getHeader() + "-" + (i + 1));
    }
  }
}
