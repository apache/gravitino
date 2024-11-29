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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.CreateFileset;
import org.apache.gravitino.cli.commands.DeleteFileset;
import org.apache.gravitino.cli.commands.FilesetDetails;
import org.apache.gravitino.cli.commands.ListFilesets;
import org.apache.gravitino.cli.commands.UpdateFilesetComment;
import org.apache.gravitino.cli.commands.UpdateFilesetName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestFilesetCommands {
  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
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
}
