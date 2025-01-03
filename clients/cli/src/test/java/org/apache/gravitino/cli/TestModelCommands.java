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
import static org.mockito.ArgumentMatchers.*;
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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.*;
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
        .newListModel(
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"));
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
        .newListModel(
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            isNull(),
            isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MISSING_NAME
            + "\n"
            + "Missing required argument(s): "
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
        .newListModel(
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            eq("catalog"),
            isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            eq("model"));
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            isNull(),
            isNull(),
            isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MISSING_NAME
            + "\n"
            + "Missing required argument(s): "
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            eq("catalog"),
            isNull(),
            isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
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
            eq(GravitinoCommandLine.DEFAULT_URL),
            eq(false),
            eq("metalake_demo"),
            eq("catalog"),
            eq("schema"),
            isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + "Missing required argument(s): "
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "catalog", "schema", "model");
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testCreateModelCommand() {
    CreateModel mockCreate = mock(CreateModel.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema.model");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                    mockCommandLine, mockOptions, CommandEntities.MODEL, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
            .newCreateModel(
                eq(GravitinoCommandLine.DEFAULT_URL),
                eq(false),
                eq("metalake_demo"),
                eq("catalog"),
                eq("schema"),
                eq("model"),
                eq("comment"),
                any());
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
                    GravitinoCommandLine.DEFAULT_URL,
                    false,
                    false,
                    "metalake_demo",
                    "catalog",
                    "schema",
                    "model");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }
}
