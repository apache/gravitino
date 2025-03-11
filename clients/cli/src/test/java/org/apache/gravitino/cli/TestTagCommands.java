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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
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
import org.apache.gravitino.cli.commands.CreateTag;
import org.apache.gravitino.cli.commands.DeleteTag;
import org.apache.gravitino.cli.commands.ListAllTags;
import org.apache.gravitino.cli.commands.ListEntityTags;
import org.apache.gravitino.cli.commands.ListTagProperties;
import org.apache.gravitino.cli.commands.RemoveAllTags;
import org.apache.gravitino.cli.commands.RemoveTagProperty;
import org.apache.gravitino.cli.commands.SetTagProperty;
import org.apache.gravitino.cli.commands.TagDetails;
import org.apache.gravitino.cli.commands.TagEntity;
import org.apache.gravitino.cli.commands.UntagEntity;
import org.apache.gravitino.cli.commands.UpdateTagComment;
import org.apache.gravitino.cli.commands.UpdateTagName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

class TestTagCommands {
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
  void testListTagsCommand() {
    ListAllTags mockList = mock(ListAllTags.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListTags(any(CommandContext.class), eq("metalake_demo"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testTagDetailsCommand() {
    TagDetails mockDetails = mock(TagDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    when(mockCommandLine.getOptionValue(GravitinoOptions.TAG)).thenReturn("tagA");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newTagDetails(any(CommandContext.class), eq("metalake_demo"), eq("tagA"));
    doReturn(mockDetails).when(mockDetails).validate();
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testTagDetailsCommandWithMultipleTag() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newTagDetails(any(CommandContext.class), eq("metalake_demo"), any());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR, output);
  }

  @Test
  void testCreateTagCommand() {
    CreateTag mockCreate = mock(CreateTag.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateTags(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(argument -> argument.length == 1 && argument[0].equals("tagA")),
            eq("comment"));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testCreateCommandWithoutTagOption() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    CreateTag spyCreate = spy(new CreateTag(mockContext, "metalake_demo", null, "comment"));

    assertThrows(RuntimeException.class, spyCreate::validate);
    verify(spyCreate, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_TAG, output);
  }

  @Test
  void testCreateTagsCommand() {
    CreateTag mockCreate = mock(CreateTag.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateTags(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(
                argument ->
                    argument.length == 2
                        && argument[0].equals("tagA")
                        && argument[1].equals("tagB")),
            eq("comment"));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testCreateTagCommandNoComment() {
    CreateTag mockCreate = mock(CreateTag.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateTags(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(argument -> argument.length == 1),
            isNull());
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteTagCommand() {
    DeleteTag mockDelete = mock(DeleteTag.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteTag(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(argument -> argument.length == 1 && argument[0].equals("tagA")));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteTagsCommand() {
    DeleteTag mockDelete = mock(DeleteTag.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteTag(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(
                argument ->
                    argument.length == 2
                        && argument[0].equals("tagA")
                        && argument[1].equals("tagB")));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteTagForceCommand() {
    DeleteTag mockDelete = mock(DeleteTag.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteTag(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(argument -> argument.length == 1 && argument[0].equals("tagA")));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testSetTagPropertyCommand() {
    SetTagProperty mockSetProperty = mock(SetTagProperty.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    when(mockCommandLine.hasOption(GravitinoOptions.VALUE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.VALUE)).thenReturn("value");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.SET));
    doReturn(mockSetProperty)
        .when(commandLine)
        .newSetTagProperty(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("tagA"),
            eq("property"),
            eq("value"));
    doReturn(mockSetProperty).when(mockSetProperty).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperty).handle();
  }

  @Test
  void testSetTagPropertyCommandWithoutPropertyAndValue() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTagProperty spySetProperty =
        spy(new SetTagProperty(mockContext, "metalake_demo", "tagA", null, null));
    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MISSING_PROPERTY_AND_VALUE);
  }

  @Test
  void testSetTagPropertyCommandWithoutPropertyOption() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTagProperty spySetProperty =
        spy(new SetTagProperty(mockContext, "metalake_demo", "tagA", null, "value"));
    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MISSING_PROPERTY);
  }

  @Test
  void testSetTagPropertyCommandWithoutValueOption() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTagProperty spySetProperty =
        spy(new SetTagProperty(mockContext, "metalake_demo", "tagA", "property", null));
    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MISSING_VALUE);
  }

  @Test
  void testSetMultipleTagPropertyCommandError() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    when(mockCommandLine.hasOption(GravitinoOptions.VALUE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.VALUE)).thenReturn("value");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.SET));
    Assertions.assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newSetTagProperty(
            any(CommandContext.class), eq("metalake_demo"), any(), eq("property"), eq("value"));
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR, output);
  }

  @Test
  void testRemoveTagPropertyCommand() {
    RemoveTagProperty mockRemoveProperty = mock(RemoveTagProperty.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.REMOVE));
    doReturn(mockRemoveProperty)
        .when(commandLine)
        .newRemoveTagProperty(
            any(CommandContext.class), eq("metalake_demo"), eq("tagA"), eq("property"));
    doReturn(mockRemoveProperty).when(mockRemoveProperty).validate();
    commandLine.handleCommandLine();
    verify(mockRemoveProperty).handle();
  }

  @Test
  void testRemoveTagPropertyCommandWithMultipleTags() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.REMOVE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newRemoveTagProperty(
            any(CommandContext.class), eq("metalake_demo"), any(), eq("property"));
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR, output);
  }

  @Test
  void testListTagPropertiesCommand() {
    ListTagProperties mockListProperties = mock(ListTagProperties.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.PROPERTIES));
    doReturn(mockListProperties)
        .when(commandLine)
        .newListTagProperties(any(CommandContext.class), eq("metalake_demo"), eq("tagA"));
    doReturn(mockListProperties).when(mockListProperties).validate();
    commandLine.handleCommandLine();
    verify(mockListProperties).handle();
  }

  @Test
  void testDeleteAllTagCommand() {
    RemoveAllTags mockRemoveAllTags = mock(RemoveAllTags.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(false);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.table");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.REMOVE));
    doReturn(mockRemoveAllTags)
        .when(commandLine)
        .newRemoveAllTags(any(CommandContext.class), eq("metalake_demo"), any(FullName.class));
    doReturn(mockRemoveAllTags).when(mockRemoveAllTags).validate();
    commandLine.handleCommandLine();
    verify(mockRemoveAllTags).handle();
  }

  @Test
  void testUpdateTagCommentCommand() {
    UpdateTagComment mockUpdateComment = mock(UpdateTagComment.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("new comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.UPDATE));
    doReturn(mockUpdateComment)
        .when(commandLine)
        .newUpdateTagComment(
            any(CommandContext.class), eq("metalake_demo"), eq("tagA"), eq("new comment"));
    doReturn(mockUpdateComment).when(mockUpdateComment).validate();
    commandLine.handleCommandLine();
    verify(mockUpdateComment).handle();
  }

  @Test
  void testUpdateTagCommentCommandWithMultipleTags() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("new comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.UPDATE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newUpdateTagComment(
            any(CommandContext.class), eq("metalake_demo"), any(), eq("new comment"));
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR, output);
  }

  @Test
  void testUpdateTagNameCommand() {
    UpdateTagName mockUpdateName = mock(UpdateTagName.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("tagB");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.UPDATE));
    doReturn(mockUpdateName)
        .when(commandLine)
        .newUpdateTagName(any(CommandContext.class), eq("metalake_demo"), eq("tagA"), eq("tagB"));
    doReturn(mockUpdateName).when(mockUpdateName).validate();
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }

  @Test
  void testUpdateTagNameCommandWithMultipleTags() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("tagC");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.UPDATE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newUpdateTagName(any(CommandContext.class), eq("metalake_demo"), any(), eq("tagC"));
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR, output);
  }

  @Test
  void testListEntityTagsCommand() {
    ListEntityTags mockListTags = mock(ListEntityTags.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.table");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.LIST));
    doReturn(mockListTags)
        .when(commandLine)
        .newListEntityTags(any(CommandContext.class), eq("metalake_demo"), any(FullName.class));
    doReturn(mockListTags).when(mockListTags).validate();
    commandLine.handleCommandLine();
    verify(mockListTags).handle();
  }

  @Test
  void testTagEntityCommand() {
    TagEntity mockTagEntity = mock(TagEntity.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.table");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG)).thenReturn(new String[] {"tagA"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.SET));
    doReturn(mockTagEntity)
        .when(commandLine)
        .newTagEntity(
            any(CommandContext.class),
            eq("metalake_demo"),
            any(),
            argThat(
                new ArgumentMatcher<String[]>() {
                  @Override
                  public boolean matches(String[] argument) {
                    return argument != null && argument.length > 0 && "tagA".equals(argument[0]);
                  }
                }));
    doReturn(mockTagEntity).when(mockTagEntity).validate();
    commandLine.handleCommandLine();
    verify(mockTagEntity).handle();
  }

  @Test
  void testTagEntityCommandWithoutName() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    TagEntity spyTagEntity =
        spy(new TagEntity(mockContext, "metalake_demo", null, new String[] {"tagA"}));

    assertThrows(RuntimeException.class, spyTagEntity::validate);
    verify(spyTagEntity, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_NAME, output);
  }

  @Test
  void testTagsEntityCommand() {
    TagEntity mockTagEntity = mock(TagEntity.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.table");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.SET));
    doReturn(mockTagEntity)
        .when(commandLine)
        .newTagEntity(
            any(CommandContext.class),
            eq("metalake_demo"),
            any(),
            argThat(
                new ArgumentMatcher<String[]>() {
                  @Override
                  public boolean matches(String[] argument) {
                    return argument != null
                        && argument.length == 2
                        && "tagA".equals(argument[0])
                        && "tagB".equals(argument[1]);
                  }
                }));
    doReturn(mockTagEntity).when(mockTagEntity).validate();
    commandLine.handleCommandLine();
    verify(mockTagEntity).handle();
  }

  @Test
  void testUntagEntityCommand() {
    UntagEntity mockUntagEntity = mock(UntagEntity.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.table");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(false);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn(null);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.REMOVE));
    doReturn(mockUntagEntity)
        .when(commandLine)
        .newUntagEntity(
            any(CommandContext.class),
            eq("metalake_demo"),
            any(),
            argThat(
                new ArgumentMatcher<String[]>() {
                  @Override
                  public boolean matches(String[] argument) {
                    return argument != null && argument.length > 0 && "tagA".equals(argument[0]);
                  }
                }));
    doReturn(mockUntagEntity).when(mockUntagEntity).validate();
    commandLine.handleCommandLine();
    verify(mockUntagEntity).handle();
  }

  @Test
  void testUntagEntityCommandWithoutName() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    UntagEntity spyUntagEntity =
        spy(new UntagEntity(mockContext, "metalake_demo", null, new String[] {"tagA"}));

    assertThrows(RuntimeException.class, spyUntagEntity::validate);
    verify(spyUntagEntity, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_NAME, output);
  }

  @Test
  void testUntagsEntityCommand() {
    UntagEntity mockUntagEntity = mock(UntagEntity.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.table");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.TAG))
        .thenReturn(new String[] {"tagA", "tagB"});
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(false);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn(null);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.REMOVE));
    doReturn(mockUntagEntity)
        .when(commandLine)
        .newUntagEntity(
            any(CommandContext.class),
            eq("metalake_demo"),
            any(),
            argThat(
                new ArgumentMatcher<String[]>() {
                  @Override
                  public boolean matches(String[] argument) {
                    return argument != null
                        && argument.length == 2
                        && "tagA".equals(argument[0])
                        && "tagB".equals(argument[1]);
                  }
                }));
    doReturn(mockUntagEntity).when(mockUntagEntity).validate();
    commandLine.handleCommandLine();
    verify(mockUntagEntity).handle();
  }

  @Test
  void testDeleteTagCommandWithoutTagOption() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    DeleteTag spyDeleteTag = spy(new DeleteTag(mockContext, "metalake", null));

    assertThrows(RuntimeException.class, spyDeleteTag::validate);
    verify(spyDeleteTag, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_TAG, output);
  }

  @Test
  void testRemoveAllTagsCommand() {
    Main.useExit = false;
    RemoveAllTags mockRemoveAllTags = mock(RemoveAllTags.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.TAG)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.table");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TAG, CommandActions.REMOVE));

    doReturn(mockRemoveAllTags)
        .when(commandLine)
        .newRemoveAllTags(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(
                argument ->
                    argument != null
                        && "catalog".equals(argument.getCatalogName())
                        && "schema".equals(argument.getSchemaName())
                        && "table".equals(argument.getTableName())));
    doReturn(mockRemoveAllTags).when(mockRemoveAllTags).validate();
    commandLine.handleCommandLine();
    verify(mockRemoveAllTags).handle();
  }

  @Test
  void testRemoveAllTagsCommandWithoutName() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    RemoveAllTags spyRemoveAllTags = spy(new RemoveAllTags(mockContext, "metalake_demo", null));

    assertThrows(RuntimeException.class, spyRemoveAllTags::validate);
    verify(spyRemoveAllTags, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_NAME, output);
  }
}
