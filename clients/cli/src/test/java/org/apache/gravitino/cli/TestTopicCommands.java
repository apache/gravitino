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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.CreateTopic;
import org.apache.gravitino.cli.commands.DeleteTopic;
import org.apache.gravitino.cli.commands.ListTopicProperties;
import org.apache.gravitino.cli.commands.ListTopics;
import org.apache.gravitino.cli.commands.RemoveTopicProperty;
import org.apache.gravitino.cli.commands.SetTopicProperty;
import org.apache.gravitino.cli.commands.TopicDetails;
import org.apache.gravitino.cli.commands.UpdateTopicComment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTopicCommands {
  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
  }

  @Test
  void testListTopicsCommand() {
    ListTopics mockList = mock(ListTopics.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListTopics(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema");
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testTopicDetailsCommand() {
    TopicDetails mockDetails = mock(TopicDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newTopicDetails(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic");
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testCreateTopicCommand() {
    CreateTopic mockCreate = mock(CreateTopic.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateTopic(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic",
            "comment");
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteTopicCommand() {
    DeleteTopic mockDelete = mock(DeleteTopic.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteTopic(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteTopicForceCommand() {
    DeleteTopic mockDelete = mock(DeleteTopic.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteTopic(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            true,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testUpdateCommentTopicCommand() {
    UpdateTopicComment mockUpdate = mock(UpdateTopicComment.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("new comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.UPDATE));
    doReturn(mockUpdate)
        .when(commandLine)
        .newUpdateTopicComment(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic",
            "new comment");
    commandLine.handleCommandLine();
    verify(mockUpdate).handle();
  }

  @Test
  void testListTopicPropertiesCommand() {
    ListTopicProperties mockListProperties = mock(ListTopicProperties.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.PROPERTIES));
    doReturn(mockListProperties)
        .when(commandLine)
        .newListTopicProperties(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic");
    commandLine.handleCommandLine();
    verify(mockListProperties).handle();
  }

  @Test
  void testSetTopicPropertyCommand() {
    SetTopicProperty mockSetProperties = mock(SetTopicProperty.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    when(mockCommandLine.hasOption(GravitinoOptions.VALUE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.VALUE)).thenReturn("value");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.SET));
    doReturn(mockSetProperties)
        .when(commandLine)
        .newSetTopicProperty(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic",
            "property",
            "value");
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }

  @Test
  void testRemoveTopicPropertyCommand() {
    RemoveTopicProperty mockSetProperties = mock(RemoveTopicProperty.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.topic");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.REMOVE));
    doReturn(mockSetProperties)
        .when(commandLine)
        .newRemoveTopicProperty(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            null,
            null,
            "metalake_demo",
            "catalog",
            "schema",
            "topic",
            "property");
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }
}
