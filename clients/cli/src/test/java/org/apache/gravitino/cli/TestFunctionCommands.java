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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.ListFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFunctionCommands {
  private static final String METALAKE_NAME = "demo_metalake";
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
  void testListFunctionsCommand() {
    ListFunctions mockList = mock(ListFunctions.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn(METALAKE_NAME);
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FUNCTION, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListFunctions(any(CommandContext.class), eq(METALAKE_NAME), any(FullName.class));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList, times(1)).handle();
  }

  @Test
  void testListFunctionsCommandWithoutMetalake() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog.schema");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FUNCTION, CommandActions.LIST));
    Assertions.assertThrows(RuntimeException.class, commandLine::handleCommandLine);

    String output = errContent.toString(StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(ErrorMessages.MISSING_METALAKE, output);
  }

  @Test
  void testListFunctionsCommandWithoutName() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn(METALAKE_NAME);
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(false);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FUNCTION, CommandActions.LIST));
    Assertions.assertThrows(RuntimeException.class, commandLine::handleCommandLine);

    String output = errContent.toString(StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        ErrorMessages.MISSING_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.CATALOG
            + ", "
            + CommandEntities.SCHEMA,
        output);
  }

  @Test
  void testListFunctionsCommandWithMalformedEntity() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn(METALAKE_NAME);
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.FUNCTION, CommandActions.LIST));
    Assertions.assertThrowsExactly(RuntimeException.class, commandLine::handleCommandLine);

    String output = errContent.toString(StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        ErrorMessages.MALFORMED_NAME
            + "\n"
            + ErrorMessages.MISSING_ENTITIES
            + CommandEntities.SCHEMA,
        output);
  }
}
