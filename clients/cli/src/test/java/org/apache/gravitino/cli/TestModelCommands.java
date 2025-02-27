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
import static org.mockito.ArgumentMatchers.argThat;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.DeleteModel;
import org.apache.gravitino.cli.commands.LinkModel;
import org.apache.gravitino.cli.commands.ListModel;
import org.apache.gravitino.cli.commands.ModelAudit;
import org.apache.gravitino.cli.commands.ModelDetails;
import org.apache.gravitino.cli.commands.RegisterModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.base.Joiner;

public class TestModelCommands {
  private final Joiner joiner = Joiner.on(", ").skipNulls();
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
  void testListModelCommand() {
    ListModel mockList = mock(ListModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.LIST));

    doReturn(mockList)
        .when(commandLine)
        .newListModel(any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("schema"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testListModelCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListModel(any(CommandContext.class), eq("metalake_demo"), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + joiner.join(Arrays.asList(CommandEntities.CATALOG, CommandEntities.SCHEMA)),
        output);
  }

  @Test
  void testListModelCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.LIST));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newListModel(any(CommandContext.class), eq("metalake_demo"), eq("catalog"), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + joiner.join(Collections.singletonList(CommandEntities.SCHEMA)),
        output);
  }

  @Test
  void testModelDetailsCommand() {
    ModelDetails mockList = mock(ModelDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.DETAILS));

    doReturn(mockList)
        .when(commandLine)
        .newModelDetails(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testModelDetailsCommandWithoutCatalog() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);

    verify(commandLine, never())
        .newModelDetails(
            any(CommandContext.class), eq("metalake_demo"), isNull(), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + joiner.join(
                Arrays.asList(
                    CommandEntities.CATALOG, CommandEntities.SCHEMA, CommandEntities.MODEL)),
        output);
  }

  @Test
  void testModelDetailsCommandWithoutSchema() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);

    verify(commandLine, never())
        .newModelDetails(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), isNull(), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + joiner.join(Arrays.asList(CommandEntities.SCHEMA, CommandEntities.MODEL)),
        output);
  }

  @Test
  void testModelDetailsCommandWithoutModel() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);

    verify(commandLine, never())
        .newModelDetails(
            any(CommandContext.class), eq("metalake_demo"), eq("catalog"), eq("schema"), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + joiner.join(Collections.singletonList(CommandEntities.MODEL)),
        output);
  }

  @Test
  void testModelAuditCommand() {
    ModelAudit mockAudit = mock(ModelAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newModelAudit(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"));
    doReturn(mockAudit).when(mockAudit).validate();
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testRegisterModelCommand() {
    RegisterModel mockCreate = mock(RegisterModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTIES)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"),
            isNull(),
            argThat(Map::isEmpty));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testRegisterModelCommandWithComment() {
    RegisterModel mockCreate = mock(RegisterModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTIES)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"),
            eq("comment"),
            argThat(Map::isEmpty));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testRegisterModelCommandWithProperties() {
    RegisterModel mockCreate = mock(RegisterModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTIES)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.PROPERTIES))
        .thenReturn(new String[] {"key1=val1", "key2" + "=val2"});
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.CREATE));

    doReturn(mockCreate)
        .when(commandLine)
        .newCreateModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"),
            isNull(),
            argThat(
                argument ->
                    argument.size() == 2
                        && argument.containsKey("key1")
                        && argument.get("key1").equals("val1")));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testRegisterModelCommandWithCommentAndProperties() {
    RegisterModel mockCreate = mock(RegisterModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTIES)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.PROPERTIES))
        .thenReturn(new String[] {"key1=val1", "key2" + "=val2"});
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.CREATE));

    doReturn(mockCreate)
        .when(commandLine)
        .newCreateModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"),
            eq("comment"),
            argThat(
                argument ->
                    argument.size() == 2
                        && argument.containsKey("key1")
                        && argument.get("key1").equals("val1")));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteModelCommand() {
    DeleteModel mockDelete = mock(DeleteModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testLinkModelCommandWithoutAlias() {
    LinkModel linkModelMock = mock(LinkModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.URI)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.URI)).thenReturn("file:///tmp/file");
    when(mockCommandLine.hasOption(GravitinoOptions.ALIAS)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.UPDATE));

    doReturn(linkModelMock)
        .when(commandLine)
        .newLinkModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"),
            eq("file:///tmp/file"),
            isNull(),
            isNull(),
            argThat(Map::isEmpty));
    doReturn(linkModelMock).when(linkModelMock).validate();
    commandLine.handleCommandLine();
    verify(linkModelMock).handle();
  }

  @Test
  void testLinkModelCommandWithAlias() {
    LinkModel linkModelMock = mock(LinkModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.URI)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.URI)).thenReturn("file:///tmp/file");
    when(mockCommandLine.hasOption(GravitinoOptions.ALIAS)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ALIAS))
        .thenReturn(new String[] {"aliasA", "aliasB"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.UPDATE));

    doReturn(linkModelMock)
        .when(commandLine)
        .newLinkModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"),
            eq("file:///tmp/file"),
            argThat(
                argument ->
                    argument.length == 2
                        && "aliasA".equals(argument[0])
                        && "aliasB".equals(argument[1])),
            isNull(),
            argThat(Map::isEmpty));
    doReturn(linkModelMock).when(linkModelMock).validate();
    commandLine.handleCommandLine();
    verify(linkModelMock).handle();
  }

  @Test
  void testLinkModelCommandWithoutURI() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    LinkModel spyLinkModel =
        spy(
            new LinkModel(
                mockContext,
                "metalake_demo",
                "catalog",
                "schema",
                "model",
                null,
                new String[] {"aliasA", "aliasB"},
                "comment",
                Collections.EMPTY_MAP));

    assertThrows(RuntimeException.class, spyLinkModel::validate);
    verify(spyLinkModel, never()).handle();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_URI, output);
  }

  @Test
  void testLinkModelCommandWithAllComponent() {
    LinkModel linkModelMock = mock(LinkModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.URI)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.URI)).thenReturn("file:///tmp/file");
    when(mockCommandLine.hasOption(GravitinoOptions.ALIAS)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ALIAS))
        .thenReturn(new String[] {"aliasA", "aliasB"});
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTIES)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.PROPERTIES))
        .thenReturn(new String[] {"key1=val1", "key2" + "=val2"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.UPDATE));

    doReturn(linkModelMock)
        .when(commandLine)
        .newLinkModel(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"),
            eq("file:///tmp/file"),
            argThat(
                argument ->
                    argument.length == 2
                        && "aliasA".equals(argument[0])
                        && "aliasB".equals(argument[1])),
            eq("comment"),
            argThat(
                argument ->
                    argument.size() == 2
                        && argument.containsKey("key1")
                        && argument.containsKey("key2")
                        && "val1".equals(argument.get("key1"))
                        && "val2".equals(argument.get("key2"))));
    doReturn(linkModelMock).when(linkModelMock).validate();
    commandLine.handleCommandLine();
    verify(linkModelMock).handle();
  }
}
