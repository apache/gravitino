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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
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
import org.apache.gravitino.cli.commands.CreateMetalake;
import org.apache.gravitino.cli.commands.DeleteMetalake;
import org.apache.gravitino.cli.commands.ListMetalakeProperties;
import org.apache.gravitino.cli.commands.ListMetalakes;
import org.apache.gravitino.cli.commands.MetalakeAudit;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.MetalakeDisable;
import org.apache.gravitino.cli.commands.MetalakeEnable;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.SetMetalakeProperty;
import org.apache.gravitino.cli.commands.UpdateMetalakeComment;
import org.apache.gravitino.cli.commands.UpdateMetalakeName;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestMetalakeCommands {
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
  void testListMetalakesCommand() {
    ListMetalakes mockList = mock(ListMetalakes.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListMetalakes(GravitinoCommandLine.DEFAULT_URL, false, null);
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testMetalakeDetailsCommand() {
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
        .newMetalakeDetails(GravitinoCommandLine.DEFAULT_URL, false, null, "metalake_demo");
    doReturn(mockDetails).when(mockDetails).validate();
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testMetalakeAuditCommand() {
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
        .newMetalakeAudit(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
    doReturn(mockAudit).when(mockAudit).validate();
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testCreateMetalakeCommand() {
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
        .newCreateMetalake(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "comment");
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testCreateMetalakeCommandNoComment() {
    CreateMetalake mockCreate = mock(CreateMetalake.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateMetalake(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", null);
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteMetalakeCommand() {
    DeleteMetalake mockDelete = mock(DeleteMetalake.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteMetalake(GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo");
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteMetalakeForceCommand() {
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
        .newDeleteMetalake(GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo");
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testSetMetalakePropertyCommand() {
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
        .newSetMetalakeProperty(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "property", "value");
    doReturn(mockSetProperty).when(mockSetProperty).validate();
    commandLine.handleCommandLine();
    verify(mockSetProperty).handle();
  }

  @Test
  void testSetMetalakePropertyCommandWithoutPropertyAndValue() {
    Main.useExit = false;
    SetMetalakeProperty metalakeProperty =
        spy(
            new SetMetalakeProperty(
                GravitinoCommandLine.DEFAULT_URL, false, "demo_metalake", null, null));

    assertThrows(RuntimeException.class, metalakeProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals("Missing --property and --value options.", errOutput);
  }

  @Test
  void testSetMetalakePropertyCommandWithoutProperty() {
    Main.useExit = false;
    SetMetalakeProperty metalakeProperty =
        spy(
            new SetMetalakeProperty(
                GravitinoCommandLine.DEFAULT_URL, false, "demo_metalake", null, "val1"));

    assertThrows(RuntimeException.class, metalakeProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, errOutput);
  }

  @Test
  void testSetMetalakePropertyCommandWithoutValue() {
    Main.useExit = false;
    SetMetalakeProperty metalakeProperty =
        spy(
            new SetMetalakeProperty(
                GravitinoCommandLine.DEFAULT_URL, false, "demo_metalake", "property1", null));

    assertThrows(RuntimeException.class, metalakeProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_VALUE, errOutput);
  }

  @Test
  void testRemoveMetalakePropertyCommand() {
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
        .newRemoveMetalakeProperty(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "property");
    doReturn(mockRemoveProperty).when(mockRemoveProperty).validate();
    commandLine.handleCommandLine();
    verify(mockRemoveProperty).handle();
  }

  @Test
  void testRemoveMetalakePropertyCommandWithoutProperty() {
    Main.useExit = false;
    RemoveMetalakeProperty mockRemoveProperty =
        spy(
            new RemoveMetalakeProperty(
                GravitinoCommandLine.DEFAULT_URL, false, "demo_metalake", null));

    assertThrows(RuntimeException.class, mockRemoveProperty::validate);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PROPERTY, errOutput);
  }

  @Test
  void testListMetalakePropertiesCommand() {
    ListMetalakeProperties mockListProperties = mock(ListMetalakeProperties.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.PROPERTIES));
    doReturn(mockListProperties)
        .when(commandLine)
        .newListMetalakeProperties(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
    doReturn(mockListProperties).when(mockListProperties).validate();
    commandLine.handleCommandLine();
    verify(mockListProperties).handle();
  }

  @Test
  void testUpdateMetalakeCommentCommand() {
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
        .newUpdateMetalakeComment(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "new comment");
    doReturn(mockUpdateComment).when(mockUpdateComment).validate();
    commandLine.handleCommandLine();
    verify(mockUpdateComment).handle();
  }

  @Test
  void testUpdateMetalakeNameCommand() {
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
        .newUpdateMetalakeName(
            GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo", "new_name");
    doReturn(mockUpdateName).when(mockUpdateName).validate();
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }

  @Test
  void testUpdateMetalakeNameForceCommand() {
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
        .newUpdateMetalakeName(
            GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo", "new_name");
    doReturn(mockUpdateName).when(mockUpdateName).validate();
    commandLine.handleCommandLine();
    verify(mockUpdateName).handle();
  }

  @Test
  void testEnableMetalakeCommand() {
    MetalakeEnable mockEnable = mock(MetalakeEnable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ENABLE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.UPDATE));
    doReturn(mockEnable)
        .when(commandLine)
        .newMetalakeEnable(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", false);
    doReturn(mockEnable).when(mockEnable).validate();
    commandLine.handleCommandLine();
    verify(mockEnable).handle();
  }

  @Test
  void testEnableMetalakeCommandWithRecursive() {
    MetalakeEnable mockEnable = mock(MetalakeEnable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ALL)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.ENABLE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.UPDATE));
    doReturn(mockEnable)
        .when(commandLine)
        .newMetalakeEnable(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", true);
    doReturn(mockEnable).when(mockEnable).validate();
    commandLine.handleCommandLine();
    verify(mockEnable).handle();
  }

  @Test
  void testDisableMetalakeCommand() {
    MetalakeDisable mockDisable = mock(MetalakeDisable.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.DISABLE)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.UPDATE));
    doReturn(mockDisable)
        .when(commandLine)
        .newMetalakeDisable(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
    doReturn(mockDisable).when(mockDisable).validate();
    commandLine.handleCommandLine();
    verify(mockDisable).handle();
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testMetalakeWithDisableAndEnableOptions() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ENABLE)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.DISABLE)).thenReturn(true);

    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.METALAKE, CommandActions.UPDATE));

    Assert.assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newMetalakeEnable(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", false);
    verify(commandLine, never())
        .newMetalakeEnable(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", false);
    assertTrue(errContent.toString().contains("Unable to enable and disable at the same time"));
  }
}
