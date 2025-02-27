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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.CreateTable;
import org.apache.gravitino.cli.commands.DeleteTable;
import org.apache.gravitino.cli.commands.ListIndexes;
import org.apache.gravitino.cli.commands.ListTableProperties;
import org.apache.gravitino.cli.commands.ListTables;
import org.apache.gravitino.cli.commands.RemoveTableProperty;
import org.apache.gravitino.cli.commands.SetTableProperty;
import org.apache.gravitino.cli.commands.TableAudit;
import org.apache.gravitino.cli.commands.TableDetails;
import org.apache.gravitino.cli.commands.TableDistribution;
import org.apache.gravitino.cli.commands.TablePartition;
import org.apache.gravitino.cli.commands.TableSortOrder;
import org.apache.gravitino.cli.commands.UpdateTableComment;
import org.apache.gravitino.cli.commands.UpdateTableName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTableCommands {
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
  void testListTablesCommand() {
    ListTables mockList = mock(ListTables.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListTables(any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("schema"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testTableDetailsCommand() {
    TableDetails mockDetails = mock(TableDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newTableDetails(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockDetails).when(mockDetails).validate();
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testTableIndexCommand() {
    ListIndexes mockIndex = mock(ListIndexes.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.INDEX)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));
    doReturn(mockIndex)
        .when(commandLine)
        .newListIndexes(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockIndex).when(mockIndex).validate();
    commandLine.handleCommandLine();
    verify(mockIndex).handle();
  }

  @Test
  void testTablePartitionCommand() {
    TablePartition mockPartition = mock(TablePartition.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.PARTITION)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));
    doReturn(mockPartition)
        .when(commandLine)
        .newTablePartition(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockPartition).when(mockPartition).validate();
    commandLine.handleCommandLine();
    verify(mockPartition).handle();
  }

  @Test
  void testTableDistributionCommand() {
    TableDistribution mockDistribution = mock(TableDistribution.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.DISTRIBUTION)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));
    doReturn(mockDistribution)
        .when(commandLine)
        .newTableDistribution(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockDistribution).when(mockDistribution).validate();
    commandLine.handleCommandLine();
    verify(mockDistribution).handle();
  }

  @Test
  void testTableSortOrderCommand() {
    TableSortOrder mockSortOrder = mock(TableSortOrder.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.SORTORDER)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));
    doReturn(mockSortOrder)
        .when(commandLine)
        .newTableSortOrder(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockSortOrder).when(mockSortOrder).validate();
    commandLine.handleCommandLine();
    verify(mockSortOrder).handle();
  }

  @Test
  void testTableAuditCommand() {
    TableAudit mockAudit = mock(TableAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newTableAudit(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockAudit).when(mockAudit).validate();
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testDeleteTableCommand() {
    DeleteTable mockDelete = mock(DeleteTable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteTable(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteTableForceCommand() {
    DeleteTable mockDelete = mock(DeleteTable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteTable(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testListTablePropertiesCommand() {
    ListTableProperties mockListProperties = mock(ListTableProperties.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.PROPERTIES));
    doReturn(mockListProperties)
        .when(commandLine)
        .newListTableProperties(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"));
    doReturn(mockListProperties).when(mockListProperties).validate();
    commandLine.handleCommandLine();
    verify(mockListProperties).handle();
  }

  @Test
  void testSetTablePropertyCommand() {
    SetTableProperty mockSetProperties = mock(SetTableProperty.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.user");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    when(mockCommandLine.hasOption(GravitinoOptions.VALUE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.VALUE)).thenReturn("value");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.SET));
    doReturn(mockSetProperties)
        .when(commandLine)
        .newSetTableProperty(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("user"),
            eq("property"),
            eq("value"));
    doReturn(mockSetProperties).when(mockSetProperties).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }

  @Test
  void testSetTablePropertyCommandWithoutPropertyAndValue() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTableProperty spySetProperty =
        spy(
            new SetTableProperty(
                mockContext, "metalake_demo", "catalog", "schema", "table", null, null));
    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY_AND_VALUE, output);
  }

  @Test
  void testSetTablePropertyCommandWithoutProperty() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTableProperty spySetProperty =
        spy(
            new SetTableProperty(
                mockContext, "metalake_demo", "catalog", "schema", "table", null, "value"));

    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, output);
  }

  @Test
  void testSetTablePropertyCommandWithoutValue() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTableProperty spySetProperty =
        spy(
            new SetTableProperty(
                mockContext, "metalake_demo", "catalog", "schema", "table", "property", null));

    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_VALUE, output);
  }

  @Test
  void testRemoveTablePropertyCommand() {
    RemoveTableProperty mockSetProperties = mock(RemoveTableProperty.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.REMOVE));
    doReturn(mockSetProperties)
        .when(commandLine)
        .newRemoveTableProperty(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"),
            eq("property"));
    doReturn(mockSetProperties).when(mockSetProperties).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }

  @Test
  void testRemoveTablePropertyCommandWithoutProperty() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    RemoveTableProperty spyRemoveProperty =
        spy(
            new RemoveTableProperty(
                mockContext, "metalake_demo", "catalog", "schema", "table", null));

    assertThrows(RuntimeException.class, spyRemoveProperty::validate);
    verify(spyRemoveProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, output);
  }

  @Test
  void testUpdateTableCommentsCommand() {
    UpdateTableComment mockUpdate = mock(UpdateTableComment.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("New comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.UPDATE));
    doReturn(mockUpdate)
        .when(commandLine)
        .newUpdateTableComment(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"),
            eq("New comment"));
    doReturn(mockUpdate).when(mockUpdate).validate();
    commandLine.handleCommandLine();
    verify(mockUpdate).handle();
  }

  @Test
  void testupdateTableNmeCommand() {
    UpdateTableName mockUpdate = mock(UpdateTableName.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("people");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.UPDATE));
    doReturn(mockUpdate)
        .when(commandLine)
        .newUpdateTableName(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"),
            eq("people"));
    doReturn(mockUpdate).when(mockUpdate).validate();
    commandLine.handleCommandLine();
    verify(mockUpdate).handle();
  }

  @Test
  void testCreateTable() {
    CreateTable mockCreate = mock(CreateTable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.users");
    when(mockCommandLine.hasOption(GravitinoOptions.COLUMNFILE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COLUMNFILE)).thenReturn("users.csv");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateTable(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("users"),
            eq("users.csv"),
            eq("comment"));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testCreateTableWithoutFile() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    CreateTable spyCreate =
        spy(
            new CreateTable(
                mockContext, "metalake_demo", "catalog", "schema", "table", null, "comment"));

    assertThrows(RuntimeException.class, spyCreate::validate);
    verify(spyCreate, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_COLUMN_FILE, output);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListTableWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListTables(any(CommandContext.class), eq("metalake_demo"), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.CATALOG
            + ", "
            + CommandEntities.SCHEMA);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListTableWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListTables(any(CommandContext.class), eq("metalake_demo"), eq("catalog"), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.SCHEMA);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDetailTableWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newTableDetails(
            any(CommandContext.class), eq("metalake_demo"), isNull(), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.CATALOG
            + ", "
            + CommandEntities.SCHEMA
            + ", "
            + CommandEntities.TABLE);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDetailTableWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));
    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newTableDetails(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.SCHEMA
            + ", "
            + CommandEntities.TABLE);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDetailTableWithoutTable() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TABLE, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newTableDetails(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("schema"), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.TABLE);
  }
}
