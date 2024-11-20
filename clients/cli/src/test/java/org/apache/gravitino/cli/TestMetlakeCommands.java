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
import org.apache.gravitino.cli.commands.CreateMetalake;
import org.apache.gravitino.cli.commands.DeleteMetalake;
import org.apache.gravitino.cli.commands.ListMetalakeProperties;
import org.apache.gravitino.cli.commands.ListMetalakes;
import org.apache.gravitino.cli.commands.MetalakeAudit;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.SetMetalakeProperty;
import org.apache.gravitino.cli.commands.UpdateMetalakeComment;
import org.apache.gravitino.cli.commands.UpdateMetalakeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GravitinoCommandLineTest {
  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
  }

  @Test
  void listMetalakesCommand() {
    ListMetalakes mockList = mock(ListMetalakes.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .createListMetalakes(GravitinoCommandLine.DEFAULT_URL, false);
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void metalakeDetailsCommand() {
    MetalakeDetails mockDetails = mock(MetalakeDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .createMetalakeDetails(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void metalakeAuditCommand() {
    MetalakeAudit mockAudit = mock(MetalakeAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .createMetalakeAudit(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void createMetalakeCommand() {
    CreateMetalake mockCreate = mock(CreateMetalake.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .createCreateMetalake(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "comment");
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void deleteMetalakeCommand() {
    DeleteMetalake mockDelete = mock(DeleteMetalake.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .createDeleteMetalake(GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void deleteMetalakeForceCommand() {
    DeleteMetalake mockDelete = mock(DeleteMetalake.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .createDeleteMetalake(GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void setMetalakePropertyCommand() {
    SetMetalakeProperty mockSetProperty = mock(SetMetalakeProperty.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    when(mockCommandLine.hasOption(GravitinoOptions.VALUE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.VALUE)).thenReturn("value");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.SET));
    doReturn(mockSetProperty)
        .when(commandLine)
        .createSetMetalakeProperty(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "property", "value");
    commandLine.handleCommandLine();
    verify(mockSetProperty).handle();
  }

  @Test
  void removeMetalakePropertyCommand() {
    RemoveMetalakeProperty mockRemoveProperty = mock(RemoveMetalakeProperty.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.PROPERTY)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.PROPERTY)).thenReturn("property");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.REMOVE));
    doReturn(mockRemoveProperty)
        .when(commandLine)
        .createRemoveMetalakeProperty(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "property");
    commandLine.handleCommandLine();
    verify(mockRemoveProperty).handle();
  }

  @Test
  void listMetalakePropertiesCommand() {
    ListMetalakeProperties mockListProperties = mock(ListMetalakeProperties.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.PROPERTIES));
    doReturn(mockListProperties)
        .when(commandLine)
        .createListMetalakeProperties(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
    commandLine.handleCommandLine();
    verify(mockListProperties).handle();
  }

  @Test
  void updateMetalakeCommentCommand() {
    UpdateMetalakeComment mockUpdateComment = mock(UpdateMetalakeComment.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.COMMENT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.COMMENT)).thenReturn("new comment");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.UPDATE));
    doReturn(mockUpdateComment)
        .when(commandLine)
        .createUpdateMetalakeComment(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "new comment");
    commandLine.handleCommandLine();
    verify(mockUpdateComment).handle();
  }

  @Test
  void updateMetalakeNameCommand() {
    UpdateMetalakeName mockUpdateName = mock(UpdateMetalakeName.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("new_name");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.UPDATE));
    doReturn(mockUpdateName)
        .when(commandLine)
        .createUpdateMetalakeName(
            GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo", "new_name");
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }

  @Test
  void updateMetalakeNameForceCommand() {
    UpdateMetalakeName mockUpdateName = mock(UpdateMetalakeName.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.RENAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.RENAME)).thenReturn("new_name");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.UPDATE));
    doReturn(mockUpdateName)
        .when(commandLine)
        .createUpdateMetalakeName(
            GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo", "new_name");
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }
}
