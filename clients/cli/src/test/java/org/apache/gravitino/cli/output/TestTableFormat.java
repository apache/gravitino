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

import org.apache.gravitino.cli.outputs.Column;
import org.apache.gravitino.cli.outputs.HorizontalAlign;
import org.apache.gravitino.cli.outputs.OutputProperty;
import org.apache.gravitino.cli.outputs.Style;
import org.apache.gravitino.cli.outputs.TableFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableFormat {
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.FANCY);

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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.FANCY2);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.BASIC2);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.BASIC2);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.BASIC2);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.BASIC2);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.BASIC2);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.BASIC2);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();
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
    property.setStyle(Style.FANCY);
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

    String outputString = tableFormat.formatTable(columnA, columnB).trim();

    Assertions.assertEquals(
        "╔══════════╤═════════╗\n"
            + "║ METALAKE │ COMMENT ║\n"
            + "╠══════════╪═════════╣\n"
            + "╚══════════╧═════════╝",
        outputString);
  }

  @Test
  void testMetalakeTableOutput() {}

  @Test
  void testMetalakesTableOutput() {}

  @Test
  void testCatalogTableOutput() {}

  @Test
  void testCatalogsTableOutput() {}

  @Test
  void testSchemaTableOutput() {}

  @Test
  void testSchemasTableOutput() {}

  @Test
  void testTableTableOutput() {}

  @Test
  void testTablesTableOutput() {}

  private void addRepeatedCells(Column column, int count) {
    for (int i = 0; i < count; i++) {
      column.addCell(column.getHeader() + "-" + (i + 1));
    }
  }
}
