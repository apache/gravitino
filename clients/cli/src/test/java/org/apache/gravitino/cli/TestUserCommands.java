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
import org.apache.gravitino.cli.commands.AddRoleToUser;
import org.apache.gravitino.cli.commands.CreateUser;
import org.apache.gravitino.cli.commands.DeleteUser;
import org.apache.gravitino.cli.commands.ListUsers;
import org.apache.gravitino.cli.commands.RemoveAllRoles;
import org.apache.gravitino.cli.commands.RemoveRoleFromUser;
import org.apache.gravitino.cli.commands.UserAudit;
import org.apache.gravitino.cli.commands.UserDetails;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestUserCommands {
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
  void testListUsersCommand() {
    ListUsers mockList = mock(ListUsers.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListUsers(any(CommandContext.class), eq("metalake_demo"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testUserDetailsCommand() {
    UserDetails mockDetails = mock(UserDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newUserDetails(any(CommandContext.class), eq("metalake_demo"), eq("user"));
    doReturn(mockDetails).when(mockDetails).validate();
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testUserAuditCommand() {
    UserAudit mockAudit = mock(UserAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("admin");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newUserAudit(any(CommandContext.class), eq("metalake_demo"), eq("admin"));
    doReturn(mockAudit).when(mockAudit).validate();
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testCreateUserCommand() {
    CreateUser mockCreate = mock(CreateUser.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateUser(any(CommandContext.class), eq("metalake_demo"), eq("user"));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteUserCommand() {
    DeleteUser mockDelete = mock(DeleteUser.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteUser(any(CommandContext.class), eq("metalake_demo"), eq("user"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteUserForceCommand() {
    DeleteUser mockDelete = mock(DeleteUser.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteUser(any(CommandContext.class), eq("metalake_demo"), eq("user"));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  void testRemoveRoleFromUserCommand() {
    RemoveRoleFromUser mockRemove = mock(RemoveRoleFromUser.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.REVOKE));
    doReturn(mockRemove)
        .when(commandLine)
        .newRemoveRoleFromUser(
            any(CommandContext.class), eq("metalake_demo"), eq("user"), eq("admin"));
    commandLine.handleCommandLine();
    verify(mockRemove).handle();
  }

  void testAddRoleToUserCommand() {
    RemoveRoleFromUser mockAdd = mock(RemoveRoleFromUser.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.GRANT));
    doReturn(mockAdd)
        .when(commandLine)
        .newRemoveRoleFromUser(
            any(CommandContext.class), eq("metalake_demo"), eq("user"), eq("admin"));
    commandLine.handleCommandLine();
    verify(mockAdd).handle();
  }

  @Test
  void testRemoveRolesFromUserCommand() {
    RemoveRoleFromUser mockRemoveFirstRole = mock(RemoveRoleFromUser.class);
    RemoveRoleFromUser mockRemoveSecondRole = mock(RemoveRoleFromUser.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE))
        .thenReturn(new String[] {"admin", "role1"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.REVOKE));
    // Verify first role
    doReturn(mockRemoveFirstRole)
        .when(commandLine)
        .newRemoveRoleFromUser(
            any(CommandContext.class), eq("metalake_demo"), eq("user"), eq("admin"));

    // Verify second role
    doReturn(mockRemoveSecondRole)
        .when(commandLine)
        .newRemoveRoleFromUser(
            any(CommandContext.class), eq("metalake_demo"), eq("user"), eq("role1"));

    doReturn(mockRemoveFirstRole).when(mockRemoveFirstRole).validate();
    doReturn(mockRemoveSecondRole).when(mockRemoveSecondRole).validate();
    commandLine.handleCommandLine();

    verify(mockRemoveSecondRole).handle();
    verify(mockRemoveFirstRole).handle();
  }

  @Test
  void removeAllRolesFromUserCommand() {
    RemoveAllRoles mockRemove = mock(RemoveAllRoles.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    when(mockCommandLine.hasOption(GravitinoOptions.ALL)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.REVOKE));
    doReturn(mockRemove)
        .when(commandLine)
        .newRemoveAllRoles(any(CommandContext.class), eq("metalake_demo"), eq("user"), any());
    doReturn(mockRemove).when(mockRemove).validate();
    commandLine.handleCommandLine();
    verify(mockRemove).handle();
  }

  @Test
  void testAddRolesToUserCommand() {
    AddRoleToUser mockAddFirstRole = mock(AddRoleToUser.class);
    AddRoleToUser mockAddSecondRole = mock(AddRoleToUser.class);

    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.USER)).thenReturn("user");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE))
        .thenReturn(new String[] {"admin", "role1"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.GRANT));
    // Verify first role
    doReturn(mockAddFirstRole)
        .when(commandLine)
        .newAddRoleToUser(any(CommandContext.class), eq("metalake_demo"), eq("user"), eq("admin"));

    // Verify second role
    doReturn(mockAddSecondRole)
        .when(commandLine)
        .newAddRoleToUser(any(CommandContext.class), eq("metalake_demo"), eq("user"), eq("role1"));

    doReturn(mockAddFirstRole).when(mockAddFirstRole).validate();
    doReturn(mockAddSecondRole).when(mockAddSecondRole).validate();
    commandLine.handleCommandLine();

    verify(mockAddFirstRole).handle();
    verify(mockAddSecondRole).handle();
  }

  @Test
  void testDeleteUserWithoutUserOption() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.USER)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.USER, CommandActions.DELETE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newDeleteUser(any(CommandContext.class), eq("metalake_demo"), isNull());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MISSING_USER);
  }
}
