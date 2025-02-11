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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.CatalogAudit;
import org.apache.gravitino.cli.commands.CatalogDetails;
import org.apache.gravitino.cli.commands.CatalogDisable;
import org.apache.gravitino.cli.commands.CatalogEnable;
import org.apache.gravitino.cli.commands.CreateCatalog;
import org.apache.gravitino.cli.commands.DeleteCatalog;
import org.apache.gravitino.cli.commands.ListCatalogProperties;
import org.apache.gravitino.cli.commands.ListCatalogs;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.SetCatalogProperty;
import org.apache.gravitino.cli.commands.UpdateCatalogComment;
import org.apache.gravitino.cli.commands.UpdateCatalogName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestCatalogCommands {
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
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testListCatalogsCommand() {
    ListCatalogs mockList = mock(ListCatalogs.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListCatalogs(any(CommandContext.class), eq("metalake_demo"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testCatalogDetailsCommand() {
    CatalogDetails mockDetails = mock(CatalogDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newCatalogDetails(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    doReturn(mockDetails).when(mockDetails).validate();
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testCatalogAuditCommand() {
    CatalogAudit mockAudit = mock(CatalogAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newCatalogAudit(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    doReturn(mockAudit).when(mockAudit).validate();
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testCreateCatalogCommand() {
    HashMap<String, String> map = new HashMap<>();
    CreateCatalog mockCreate = mock(CreateCatalog.class);
    String[] props = {"key1=value1", "key2=value2"};
    map.put("key1", "value1");
    map.put("key2", "value2");

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    when(mockCommandLine.hasOption(GravitinoOptions.PROVIDER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROVIDER)).thenReturn("postgres");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTIES)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.PROPERTIES)).thenReturn(props);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateCatalog(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("postgres"),
            eq("comment"),
            argThat(
                stringStringMap ->
                    stringStringMap.containsKey("key1")
                        && stringStringMap.get("key1").equals(map.get("key1"))
                        && stringStringMap.containsKey("key2")
                        && stringStringMap.get("key2").equals("value2")));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testCreateCatalogCommandWithoutProvider() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn("http://localhost:8080");
    CreateCatalog mockCreateCatalog =
        spy(new CreateCatalog(mockContext, "metalake_demo", "catalog", null, "comment", null));

    assertThrows(RuntimeException.class, mockCreateCatalog::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROVIDER, errOutput);
  }

  @Test
  void testDeleteCatalogCommand() {
    DeleteCatalog mockDelete = mock(DeleteCatalog.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteCatalog(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteCatalogForceCommand() {
    DeleteCatalog mockDelete = mock(DeleteCatalog.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteCatalog(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testSetCatalogPropertyCommand() {
    SetCatalogProperty mockSetProperty = mock(SetCatalogProperty.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    when(mockCommandLine.hasOption(GravitinoOptions.VALUE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.VALUE)).thenReturn("value");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.SET));
    doReturn(mockSetProperty)
        .when(commandLine)
        .newSetCatalogProperty(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("property"),
            eq("value"));
    doReturn(mockSetProperty).when(mockSetProperty).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperty).handle();
  }

  @Test
  void testSetCatalogPropertyCommandWithoutPropertyAndValue() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn("http://localhost:8080");
    SetCatalogProperty mockSetProperty =
        spy(new SetCatalogProperty(mockContext, "metalake_demo", "catalog", null, null));

    assertThrows(RuntimeException.class, mockSetProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals("Missing --property and --value options.", errOutput);
  }

  @Test
  void testSetCatalogPropertyCommandWithoutProperty() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn("http://localhost:8080");
    SetCatalogProperty mockSetProperty =
        spy(new SetCatalogProperty(mockContext, "metalake_demo", "catalog", null, "value"));

    assertThrows(RuntimeException.class, mockSetProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, errOutput);
  }

  @Test
  void testSetCatalogPropertyCommandWithoutValue() {
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn("http://localhost:8080");
    SetCatalogProperty mockSetProperty =
        spy(new SetCatalogProperty(mockContext, "metalake_demo", "catalog", "property", null));

    assertThrows(RuntimeException.class, mockSetProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_VALUE, errOutput);
  }

  @Test
  void testRemoveCatalogPropertyCommand() {
    RemoveCatalogProperty mockRemoveProperty = mock(RemoveCatalogProperty.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.REMOVE));
    doReturn(mockRemoveProperty)
        .when(commandLine)
        .newRemoveCatalogProperty(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("property"));
    doReturn(mockRemoveProperty).when(mockRemoveProperty).validate();
    commandLine.handleCommandLine();
    verify(mockRemoveProperty).handle();
  }

  @Test
  void testRemoveCatalogPropertyCommandWithoutProperty() {
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn("http://localhost:8080");
    RemoveCatalogProperty mockRemoveProperty =
        spy(new RemoveCatalogProperty(mockContext, "metalake_demo", "catalog", null));

    assertThrows(RuntimeException.class, mockRemoveProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, errOutput);
  }

  @Test
  void testListCatalogPropertiesCommand() {
    ListCatalogProperties mockListProperties = mock(ListCatalogProperties.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.PROPERTIES));
    doReturn(mockListProperties)
        .when(commandLine)
        .newListCatalogProperties(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    doReturn(mockListProperties).when(mockListProperties).validate();
    commandLine.handleCommandLine();
    verify(mockListProperties).handle();
  }

  @Test
  void testUpdateCatalogCommentCommand() {
    UpdateCatalogComment mockUpdateComment = mock(UpdateCatalogComment.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("new comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.UPDATE));
    doReturn(mockUpdateComment)
        .when(commandLine)
        .newUpdateCatalogComment(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("new comment"));
    doReturn(mockUpdateComment).when(mockUpdateComment).validate();
    commandLine.handleCommandLine();
    verify(mockUpdateComment).handle();
  }

  @Test
  void testUpdateCatalogNameCommand() {
    UpdateCatalogName mockUpdateName = mock(UpdateCatalogName.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("new_name");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.UPDATE));
    doReturn(mockUpdateName)
        .when(commandLine)
        .newUpdateCatalogName(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("new_name"));
    doReturn(mockUpdateName).when(mockUpdateName).validate();
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testCatalogDetailsCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newCatalogDetails(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.CATALOG);
  }

  @Test
  void testEnableCatalogCommand() {
    CatalogEnable mockEnable = mock(CatalogEnable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.ENABLE)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.UPDATE));
    doReturn(mockEnable)
        .when(commandLine)
        .newCatalogEnable(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), anyBoolean());
    doReturn(mockEnable).when(mockEnable).validate();
    commandLine.handleCommandLine();
    verify(mockEnable).handle();
  }

  @Test
  void testEnableCatalogCommandWithRecursive() {
    CatalogEnable mockEnable = mock(CatalogEnable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.ALL)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.ENABLE)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.UPDATE));
    doReturn(mockEnable)
        .when(commandLine)
        .newCatalogEnable(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), anyBoolean());
    doReturn(mockEnable).when(mockEnable).validate();
    commandLine.handleCommandLine();
    verify(mockEnable).handle();
  }

  @Test
  void testDisableCatalogCommand() {
    CatalogDisable mockDisable = mock(CatalogDisable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.DISABLE)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.UPDATE));
    doReturn(mockDisable)
        .when(commandLine)
        .newCatalogDisable(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    doReturn(mockDisable).when(mockDisable).validate();
    commandLine.handleCommandLine();
    verify(mockDisable).handle();
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testCatalogWithDisableAndEnableOptions() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.DISABLE)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.ENABLE)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.UPDATE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newCatalogEnable(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), anyBoolean());
    verify(commandLine, never())
        .newCatalogDisable(any(CommandContext.class), eq("metalake_demo"), eq("catalog"));
    assertTrue(errContent.toString().contains(ErrorMessages.INVALID_ENABLE_DISABLE));
  }
}
