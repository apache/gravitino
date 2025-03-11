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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.AddRoleToGroup;
import org.apache.gravitino.cli.commands.CreateGroup;
import org.apache.gravitino.cli.commands.DeleteGroup;
import org.apache.gravitino.cli.commands.GroupAudit;
import org.apache.gravitino.cli.commands.GroupDetails;
import org.apache.gravitino.cli.commands.ListGroups;
import org.apache.gravitino.cli.commands.RemoveAllRoles;
import org.apache.gravitino.cli.commands.RemoveRoleFromGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestGroupCommands {
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
  void testListGroupsCommand() {
    ListGroups mockList = mock(ListGroups.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListGroups(any(CommandContext.class), eq("metalake_demo"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testGroupDetailsCommand() {
    GroupDetails mockDetails = mock(GroupDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newGroupDetails(any(CommandContext.class), eq("metalake_demo"), eq("groupA"));
    doReturn(mockDetails).when(mockDetails).validate();
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testGroupAuditCommand() {
    GroupAudit mockAudit = mock(GroupAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("group");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newGroupAudit(any(CommandContext.class), eq("metalake_demo"), eq("group"));
    doReturn(mockAudit).when(mockAudit).validate();
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testCreateGroupCommand() {
    CreateGroup mockCreate = mock(CreateGroup.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateGroup(any(CommandContext.class), eq("metalake_demo"), eq("groupA"));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteGroupCommand() {
    DeleteGroup mockDelete = mock(DeleteGroup.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteGroup(any(CommandContext.class), eq("metalake_demo"), eq("groupA"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteGroupForceCommand() {
    DeleteGroup mockDelete = mock(DeleteGroup.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteGroup(any(CommandContext.class), eq("metalake_demo"), eq("groupA"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  void testRemoveRoleFromGroupCommand() {
    RemoveRoleFromGroup mockRemove = mock(RemoveRoleFromGroup.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.REVOKE));
    doReturn(mockRemove)
        .when(commandLine)
        .newRemoveRoleFromGroup(
            any(CommandContext.class), eq("metalake_demo"), eq("groupA"), eq("admin"));
    commandLine.handleCommandLine();
    verify(mockRemove).handle();
  }

  void testAddRoleToGroupCommand() {
    AddRoleToGroup mockAdd = mock(AddRoleToGroup.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.GRANT));
    doReturn(mockAdd)
        .when(commandLine)
        .newAddRoleToGroup(
            any(CommandContext.class), eq("metalake_demo"), eq("groupA"), eq("admin"));
    commandLine.handleCommandLine();
    verify(mockAdd).handle();
  }

  @Test
  void testRemoveRolesFromGroupCommand() {
    RemoveRoleFromGroup mockRemoveFirstRole = mock(RemoveRoleFromGroup.class);
    RemoveRoleFromGroup mockRemoveSecondRole = mock(RemoveRoleFromGroup.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE))
        .thenReturn(new String[] {"admin", "role1"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.REVOKE));
    // Verify first role
    doReturn(mockRemoveFirstRole)
        .when(commandLine)
        .newRemoveRoleFromGroup(
            any(CommandContext.class), eq("metalake_demo"), eq("groupA"), eq("admin"));

    // Verify second role
    doReturn(mockRemoveSecondRole)
        .when(commandLine)
        .newRemoveRoleFromGroup(
            any(CommandContext.class), eq("metalake_demo"), eq("groupA"), eq("role1"));

    doReturn(mockRemoveFirstRole).when(mockRemoveFirstRole).validate();
    doReturn(mockRemoveSecondRole).when(mockRemoveSecondRole).validate();
    commandLine.handleCommandLine();

    verify(mockRemoveFirstRole).handle();
    verify(mockRemoveSecondRole).handle();
  }

  @Test
  void testRemoveAllRolesFromGroupCommand() {
    RemoveAllRoles mock = mock(RemoveAllRoles.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    when(mockCommandLine.hasOption(GravitinoOptions.ALL)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.REVOKE));

    doReturn(mock)
        .when(commandLine)
        .newRemoveAllRoles(
            any(CommandContext.class),
            eq("metalake_demo"),
            eq("groupA"),
            eq(CommandEntities.GROUP));
    doReturn(mock).when(mock).validate();
    commandLine.handleCommandLine();
    verify(mock).handle();
  }

  @Test
  void testAddRolesToGroupCommand() {
    AddRoleToGroup mockAddFirstRole = mock(AddRoleToGroup.class);
    AddRoleToGroup mockAddSecondRole = mock(AddRoleToGroup.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.GROUP)).thenReturn("groupA");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE))
        .thenReturn(new String[] {"admin", "role1"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.GRANT));
    // Verify first role
    doReturn(mockAddFirstRole)
        .when(commandLine)
        .newAddRoleToGroup(
            any(CommandContext.class), eq("metalake_demo"), eq("groupA"), eq("admin"));

    // Verify second role
    doReturn(mockAddSecondRole)
        .when(commandLine)
        .newAddRoleToGroup(
            any(CommandContext.class), eq("metalake_demo"), eq("groupA"), eq("role1"));

    doReturn(mockAddFirstRole).when(mockAddFirstRole).validate();
    doReturn(mockAddSecondRole).when(mockAddSecondRole).validate();
    commandLine.handleCommandLine();

    verify(mockAddSecondRole).handle();
    verify(mockAddFirstRole).handle();
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testDeleteGroupCommandWithoutGroupOption() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.GROUP)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.GROUP, CommandActions.DELETE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newDeleteGroup(any(CommandContext.class), eq("metalake_demo"), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MISSING_GROUP);
  }
}
