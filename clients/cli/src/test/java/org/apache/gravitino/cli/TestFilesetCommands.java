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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
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
import org.apache.gravitino.cli.commands.CreateFileset;
import org.apache.gravitino.cli.commands.DeleteFileset;
import org.apache.gravitino.cli.commands.FilesetDetails;
import org.apache.gravitino.cli.commands.ListFilesetProperties;
import org.apache.gravitino.cli.commands.ListFilesets;
import org.apache.gravitino.cli.commands.RemoveFilesetProperty;
import org.apache.gravitino.cli.commands.SetFilesetProperty;
import org.apache.gravitino.cli.commands.UpdateFilesetComment;
import org.apache.gravitino.cli.commands.UpdateFilesetName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestFilesetCommands {
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
  void testListFilesetsCommand() {
    ListFilesets mockList = mock(ListFilesets.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListFilesets(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "schema");
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testFilesetDetailsCommand() {
    FilesetDetails mockDetails = mock(FilesetDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newFilesetDetails(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset");
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testCreateFilesetCommand() {
    CreateFileset mockCreate = mock(CreateFileset.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateFileset(
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("fileset"),
            eq("comment"),
            any());
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteFilesetCommand() {
    DeleteFileset mockDelete = mock(DeleteFileset.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteFileset(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteFilesetForceCommand() {
    DeleteFileset mockDelete = mock(DeleteFileset.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteFileset(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            true,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testUpdateFilesetCommentCommand() {
    UpdateFilesetComment mockUpdateComment = mock(UpdateFilesetComment.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("new_comment");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.UPDATE));
    doReturn(mockUpdateComment)
        .when(commandLine)
        .newUpdateFilesetComment(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset",
            "new_comment");
    commandLine.handleCommandLine();
    verify(mockUpdateComment).handle();
  }

  @Test
  void testUpdateFilesetNameCommand() {
    UpdateFilesetName mockUpdateName = mock(UpdateFilesetName.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("new_name");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.UPDATE));
    doReturn(mockUpdateName)
        .when(commandLine)
        .newUpdateFilesetName(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset",
            "new_name");
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }

  @Test
  void testListFilesetPropertiesCommand() {
    ListFilesetProperties mockListProperties = mock(ListFilesetProperties.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.PROPERTIES));
    doReturn(mockListProperties)
        .when(commandLine)
        .newListFilesetProperties(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset");
    commandLine.handleCommandLine();
    verify(mockListProperties).handle();
  }

  @Test
  void testSetFilesetPropertyCommand() {
    SetFilesetProperty mockSetProperties = mock(SetFilesetProperty.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    when(mockCommandLine.hasOption(GravitinoOptions.VALUE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.VALUE)).thenReturn("value");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.SET));
    doReturn(mockSetProperties)
        .when(commandLine)
        .newSetFilesetProperty(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset",
            "property",
            "value");
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }

  @Test
  void testRemoveFilesetPropertyCommand() {
    RemoveFilesetProperty mockSetProperties = mock(RemoveFilesetProperty.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME))
        .thenReturn("catalog.schema.fileset");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.REMOVE));
    doReturn(mockSetProperties)
        .when(commandLine)
        .newRemoveFilesetProperty(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "catalog",
            "schema",
            "fileset",
            "property");
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListFilesetCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListFilesets(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", null, null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.CATALOG, CommandEntities.SCHEMA)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListFilesetCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListFilesets(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.SCHEMA)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testFilesetDetailCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newFilesetDetails(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", null, null, null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ")
                .join(
                    Arrays.asList(
                        CommandEntities.CATALOG, CommandEntities.SCHEMA, CommandEntities.FILESET)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testFilesetDetailCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newFilesetDetails(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", null, null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.SCHEMA, CommandEntities.FILESET)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testFilesetDetailCommandWithoutFileset() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FILESET, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newFilesetDetails(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "schema", null);
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.FILESET)));
  }
}
