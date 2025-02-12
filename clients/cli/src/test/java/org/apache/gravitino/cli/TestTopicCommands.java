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

import com.google.common.base.Joiner;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTopicCommands {
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
        .newListTopics(any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("schema"));
    doReturn(mockList).when(mockList).validate();
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"));
    doReturn(mockDetails).when(mockDetails).validate();
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"),
            eq("comment"));
    doReturn(mockCreate).when(mockCreate).validate();
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"));
    doReturn(mockDelete).when(mockDelete).validate();
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"));
    doReturn(mockDelete).when(mockDelete).validate();
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"),
            eq("new comment"));
    doReturn(mockUpdate).when(mockUpdate).validate();
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"));
    doReturn(mockListProperties).when(mockListProperties).validate();
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"),
            eq("property"),
            eq("value"));
    doReturn(mockSetProperties).when(mockSetProperties).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }

  @Test
  void testSetTopicPropertyCommandWithoutPropertyAndValue() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTopicProperty spySetProperty =
        spy(
            new SetTopicProperty(
                mockContext, "metalake_demo", "catalog", "schema", "topic", null, null));
    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY_AND_VALUE, output);
  }

  @Test
  void testSetTopicPropertyCommandWithoutProperty() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTopicProperty spySetProperty =
        spy(
            new SetTopicProperty(
                mockContext, "metalake_demo", "catalog", "schema", "topic", null, "value"));
    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, output);
  }

  @Test
  void testSetTopicPropertyCommandWithoutValue() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    SetTopicProperty spySetProperty =
        spy(
            new SetTopicProperty(
                mockContext, "metalake_demo", "catalog", "schema", "topic", "property", null));
    assertThrows(RuntimeException.class, spySetProperty::validate);
    verify(spySetProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_VALUE, output);
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
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("topic"),
            eq("property"));
    doReturn(mockSetProperties).when(mockSetProperties).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperties).handle();
  }

  @Test
  void testRemoveTopicPropertyCommandWithoutProperty() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    RemoveTopicProperty spyRemoveProperty =
        spy(
            new RemoveTopicProperty(
                mockContext, "metalake_demo", "catalog", "schema", "topic", null));

    assertThrows(RuntimeException.class, spyRemoveProperty::validate);
    verify(spyRemoveProperty, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, output);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListTopicCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListTopics(any(CommandContext.class), eq("metalake_demo"), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.CATALOG, CommandEntities.SCHEMA)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testListTopicCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListTopics(any(CommandContext.class), eq("metalake_demo"), eq("catalog"), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.SCHEMA)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testTopicDetailsCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newTopicDetails(
            any(CommandContext.class), eq("metalake_demo"), isNull(), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + Joiner.on(", ")
                .join(
                    Arrays.asList(
                        CommandEntities.CATALOG, CommandEntities.SCHEMA, CommandEntities.TOPIC)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testTopicDetailsCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newTopicDetails(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.SCHEMA, CommandEntities.TOPIC)));
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testTopicDetailsCommandWithoutTopic() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.TOPIC, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newTopicDetails(
            any(CommandContext.class), eq("metalake_demo"), eq("schema"), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        output,
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + Joiner.on(", ").join(Arrays.asList(CommandEntities.TOPIC)));
  }
}
