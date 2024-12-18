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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

class TestTagCommands {
  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
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
        .newListTags(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
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
        .newTagDetails(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "tagA");
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
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
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            new String[] {"tagA"},
            "comment");
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
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
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            new String[] {"tagA", "tagB"},
            "comment");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", new String[] {"tagA"}, null);
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
            GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo", new String[] {"tagA"});
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
            GravitinoCommandLine.DEFAULT_URL,
            false,
            false,
            "metalake_demo",
            new String[] {"tagA", "tagB"});
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
            GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo", new String[] {"tagA"});
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "tagA", "property", "value");
    commandLine.handleCommandLine();
    verify(mockSetProperty).handle();
  }

  @Test
  void testSetMultipleTagPropertyCommandError() {
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
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> commandLine.handleCommandLine(),
        "Error: The current command only supports one --tag option.");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "tagA", "property");
    commandLine.handleCommandLine();
    verify(mockRemoveProperty).handle();
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
        .newListTagProperties(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "tagA");
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
        .newRemoveAllTags(
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            any(FullName.class),
            eq(true));
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "tagA", "new comment");
    commandLine.handleCommandLine();
    verify(mockUpdateComment).handle();
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
        .newUpdateTagName(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "tagA", "tagB");
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
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
        .newListEntityTags(
            eq(GravitinoCommandLine.DEFAULT_URL), eq(false), eq("metalake_demo"), any());
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            any(),
            argThat(
                new ArgumentMatcher<String[]>() {
                  @Override
                  public boolean matches(String[] argument) {
                    return argument != null && argument.length > 0 && "tagA".equals(argument[0]);
                  }
                }));
    commandLine.handleCommandLine();
    verify(mockTagEntity).handle();
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            any(),
            argThat(
                new ArgumentMatcher<String[]>() {
                  @Override
                  public boolean matches(String[] argument) {
                    return argument != null && argument.length > 0 && "tagA".equals(argument[0]);
                  }
                }));
    commandLine.handleCommandLine();
    verify(mockUntagEntity).handle();
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
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
    commandLine.handleCommandLine();
    verify(mockUntagEntity).handle();
  }
}
