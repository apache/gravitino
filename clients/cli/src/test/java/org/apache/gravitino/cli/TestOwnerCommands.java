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
import org.apache.gravitino.cli.commands.OwnerDetails;
import org.apache.gravitino.cli.commands.SetOwner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestOwnerCommands {
  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
  }

  @Test
  void testSetOwnerUserCommand() {
    SetOwner mockSetOwner = mock(SetOwner.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("postgres");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("admin");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.OWNER)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.SET));
    doReturn(mockSetOwner)
        .when(commandLine)
        .newSetOwner(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "postgres",
            "catalog",
            "admin",
            false);
    commandLine.handleCommandLine();
    verify(mockSetOwner).handle();
  }

  @Test
  void testSetOwnerGroupCommand() {
    SetOwner mockSetOwner = mock(SetOwner.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("postgres");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("ITdept");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(false);
    when(mockCommandLine.hasOption(GravitinoOptions.OWNER)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.SET));
    doReturn(mockSetOwner)
        .when(commandLine)
        .newSetOwner(
            GravitinoCommandLine.DEFAULT_URL,
            false,
            "metalake_demo",
            "postgres",
            "catalog",
            "ITdept",
            true);
    commandLine.handleCommandLine();
    verify(mockSetOwner).handle();
  }

  @Test
  void testOwnerDetailsCommand() {
    OwnerDetails mockOwnerDetails = mock(OwnerDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("postgres");
    when(mockCommandLine.hasOption(GravitinoOptions.OWNER)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.CATALOG, CommandActions.DETAILS));
    doReturn(mockOwnerDetails)
        .when(commandLine)
        .newOwnerDetails(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "postgres", "catalog");
    commandLine.handleCommandLine();
    verify(mockOwnerDetails).handle();
  }
}
