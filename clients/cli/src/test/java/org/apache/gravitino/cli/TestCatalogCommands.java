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
import org.apache.gravitino.cli.commands.Command;
import org.apache.gravitino.cli.commands.CreateCatalog;
import org.apache.gravitino.cli.commands.DeleteCatalog;
import org.apache.gravitino.cli.commands.ListCatalogProperties;
import org.apache.gravitino.cli.commands.ListCatalogs;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.SetCatalogProperty;
import org.apache.gravitino.cli.commands.UpdateCatalogComment;
import org.apache.gravitino.cli.commands.UpdateCatalogName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
  void restoreExitFlg() {
    Main.useExit = true;
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
        .newListCatalogs(GravitinoCommandLine.DEFAULT_URL, false, null, "metalake_demo");
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
        .newCatalogDetails(
            GravitinoCommandLine.DEFAULT_URL, false, null, "metalake_demo", "catalog");
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
        .newCatalogAudit(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog");
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
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "postgres",
            "comment",
            map);
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testCreateCatalogCommandWithoutProvider() {
    Main.useExit = false;
    CreateCatalog mockCreateCatalog =
        spy(
            new CreateCatalog(
                GravitinoCommandLine.DEFAULT_URL,
                false,
                "metalake_demo",
                "catalog",
                null,
                "comment",
                null));

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
        .newDeleteCatalog(
            GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo", "catalog");
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
        .newDeleteCatalog(
            GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo", "catalog");
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
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "property",
            "value");
    doReturn(mockSetProperty).when(mockSetProperty).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperty).handle();
  }

  @Test
  void testSetCatalogPropertyCommandWithoutPropertyAndValue() {
    Main.useExit = false;
    SetCatalogProperty mockSetProperty =
        spy(
            new SetCatalogProperty(
                GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", null, null));

    assertThrows(RuntimeException.class, mockSetProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals("Missing --property and --value options.", errOutput);
  }

  @Test
  void testSetCatalogPropertyCommandWithoutProperty() {
    Main.useExit = false;
    SetCatalogProperty mockSetProperty =
        spy(
            new SetCatalogProperty(
                GravitinoCommandLine.DEFAULT_URL,
                false,
                "metalake_demo",
                "catalog",
                null,
                "value"));

    assertThrows(RuntimeException.class, mockSetProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, errOutput);
  }

  @Test
  void testSetCatalogPropertyCommandWithoutValue() {
    Main.useExit = false;
    SetCatalogProperty mockSetProperty =
        spy(
            new SetCatalogProperty(
                GravitinoCommandLine.DEFAULT_URL,
                false,
                "metalake_demo",
                "catalog",
                "property",
                null));

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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "property");
    doReturn(mockRemoveProperty).when(mockRemoveProperty).validate();
    commandLine.handleCommandLine();
    verify(mockRemoveProperty).handle();
  }

  @Test
  void testRemoveCatalogPropertyCommandWithoutProperty() {
    Main.useExit = false;
    RemoveCatalogProperty mockRemoveProperty =
        spy(
            new RemoveCatalogProperty(
                GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", null));

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
        .newListCatalogProperties(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "new comment");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "new_name");
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
        .newCatalogDetails(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            Command.OUTPUT_FORMAT_TABLE,
            "metalake_demo",
            "catalog");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", false, false);
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", false, true);
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
        .newCatalogDisable(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog");
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

    assertThrows(
        RuntimeException.class,
        () ->
            commandLine
                .newCatalogEnable(
                    GravitinoCommandLine.DEFAULT_URL,
                    false,
                    "metalake_demo",
                    "catalog",
                    true,
                    false)
                .validate());
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(ErrorMessages.INVALID_ENABLE_DISABLE, errOutput);
  }
}
