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

package org.apache.gravitino.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Joiner;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.AddColumn;
import org.apache.gravitino.cli.commands.ColumnAudit;
import org.apache.gravitino.cli.commands.DeleteColumn;
import org.apache.gravitino.cli.commands.ListColumns;
import org.apache.gravitino.cli.commands.UpdateColumnAutoIncrement;
import org.apache.gravitino.cli.commands.UpdateColumnComment;
import org.apache.gravitino.cli.commands.UpdateColumnDatatype;
import org.apache.gravitino.cli.commands.UpdateColumnDefault;
import org.apache.gravitino.cli.commands.UpdateColumnName;
import org.apache.gravitino.cli.commands.UpdateColumnNullability;
import org.apache.gravitino.cli.commands.UpdateColumnPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestColumnCommands {
  private CommandLine mockCommandLine;
  private Options mockOptions;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  void restoreExitFlg() {
    Main.useExit = true;
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testListColumnsCommand() {
    ListColumns mockList = mock(ListColumns.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListColumns(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "schema", "users");
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testColumnAuditCommand() {
    ColumnAudit mockAudit = mock(ColumnAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newColumnAudit(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name");
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testColumnDetailsCommand() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newColumnAudit(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name");

    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.UNSUPPORTED_ACTION);
  }

  @Test
  void testAddColumn() {
    AddColumn mockAddColumn = mock(AddColumn.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.DATATYPE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.DATATYPE)).thenReturn("varchar(100)");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    when(mockCommandLine.getOptionValue(GravitinoOptions.POSITION)).thenReturn(null);
    when(mockCommandLine.getOptionValue(GravitinoOptions.DEFAULT)).thenReturn(null);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.CREATE));
    doReturn(mockAddColumn)
        .when(commandLine)
        .newAddColumn(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            "varchar(100)",
            "comment",
            null,
            true,
            false,
            null);
    commandLine.handleCommandLine();
    verify(mockAddColumn).handle();
  }

  @Test
  void testDeleteColumn() {
    DeleteColumn mockDelete = mock(DeleteColumn.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteColumn(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testUpdateColumnComment() {
    UpdateColumnComment mockUpdateColumn = mock(UpdateColumnComment.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("new comment");
    when(mockCommandLine.hasOption(GravitinoOptions.NULL)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.AUTO)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.UPDATE));
    doReturn(mockUpdateColumn)
        .when(commandLine)
        .newUpdateColumnComment(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            "new comment");
    commandLine.handleCommandLine();
    verify(mockUpdateColumn).handle();
  }

  @Test
  void testUpdateColumnName() {
    UpdateColumnName mockUpdateName = mock(UpdateColumnName.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("renamed");
    when(mockCommandLine.hasOption(GravitinoOptions.NULL)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.AUTO)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.UPDATE));
    doReturn(mockUpdateName)
        .when(commandLine)
        .newUpdateColumnName(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            "renamed");
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }

  @Test
  void testUpdateColumnDatatype() {
    UpdateColumnDatatype mockUpdateDatatype = mock(UpdateColumnDatatype.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.DATATYPE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.DATATYPE)).thenReturn("varchar(250)");
    when(mockCommandLine.hasOption(GravitinoOptions.NULL)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.AUTO)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.UPDATE));
    doReturn(mockUpdateDatatype)
        .when(commandLine)
        .newUpdateColumnDatatype(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            "varchar(250)");
    commandLine.handleCommandLine();
    verify(mockUpdateDatatype).handle();
  }

  @Test
  void testUpdateColumnPosition() {
    UpdateColumnPosition mockUpdatePosition = mock(UpdateColumnPosition.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.POSITION)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.POSITION)).thenReturn("first");
    when(mockCommandLine.hasOption(GravitinoOptions.NULL)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.AUTO)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.UPDATE));
    doReturn(mockUpdatePosition)
        .when(commandLine)
        .newUpdateColumnPosition(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            "first");
    commandLine.handleCommandLine();
    verify(mockUpdatePosition).handle();
  }

  @Test
  void testUpdateColumnNullability() {
    UpdateColumnNullability mockUpdateNull = mock(UpdateColumnNullability.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.NULL)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NULL)).thenReturn("true");
    when(mockCommandLine.hasOption(GravitinoOptions.AUTO)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.UPDATE));
    doReturn(mockUpdateNull)
        .when(commandLine)
        .newUpdateColumnNullability(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            true);
    commandLine.handleCommandLine();
    verify(mockUpdateNull).handle();
  }

  @Test
  void testUpdateColumnAutoIncrement() {
    UpdateColumnAutoIncrement mockUpdateAuto = mock(UpdateColumnAutoIncrement.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.AUTO)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.AUTO)).thenReturn("true");
    when(mockCommandLine.hasOption(GravitinoOptions.NULL)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.UPDATE));
    doReturn(mockUpdateAuto)
        .when(commandLine)
        .newUpdateColumnAutoIncrement(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            true);
    commandLine.handleCommandLine();
    verify(mockUpdateAuto).handle();
  }

  @Test
  void testUpdateColumnDefault() {
    UpdateColumnDefault mockUpdateDefault = mock(UpdateColumnDefault.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.users.name");
    when(mockCommandLine.hasOption(GravitinoOptions.DEFAULT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.DEFAULT)).thenReturn("Fred Smith");
    when(mockCommandLine.hasOption(GravitinoOptions.DATATYPE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.DATATYPE)).thenReturn("varchar(100)");
    when(mockCommandLine.hasOption(GravitinoOptions.NULL)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.AUTO)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.UPDATE));
    doReturn(mockUpdateDefault)
        .when(commandLine)
        .newUpdateColumnDefault(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            "name",
            "Fred Smith",
            "varchar(100)");
    commandLine.handleCommandLine();
    verify(mockUpdateDefault).handle();
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDeleteColumnCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.DELETE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newDeleteColumn(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", null, null, null, null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ")
                .join(
                    Arrays.asList(
                        CommandEntities.CATALOG,
                        CommandEntities.SCHEMA,
                        CommandEntities.TABLE,
                        CommandEntities.COLUMN)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDeleteColumnCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.DELETE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newDeleteColumn(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", null, null, null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ")
                .join(
                    Arrays.asList(
                        CommandEntities.SCHEMA, CommandEntities.TABLE, CommandEntities.COLUMN)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDeleteColumnCommandWithoutTable() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.DELETE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newDeleteColumn(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            null,
            null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.TABLE, CommandEntities.COLUMN)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDeleteColumnCommandWithoutColumn() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.DELETE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newDeleteColumn(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "users",
            null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.COLUMN)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListColumnCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListColumns(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "schema", null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ")
                .join(
                    Arrays.asList(
                        CommandEntities.CATALOG, CommandEntities.SCHEMA, CommandEntities.TABLE)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListColumnCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListColumns(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "schema", null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.SCHEMA, CommandEntities.TABLE)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListColumnCommandWithoutTable() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.COLUMN, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListColumns(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "schema", null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + CommandEntities.TABLE);
  }
}
