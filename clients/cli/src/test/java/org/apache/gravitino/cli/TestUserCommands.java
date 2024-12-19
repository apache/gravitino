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
import org.apache.gravitino.cli.commands.AddRoleToUser;
import org.apache.gravitino.cli.commands.CreateUser;
import org.apache.gravitino.cli.commands.DeleteUser;
import org.apache.gravitino.cli.commands.ListUsers;
import org.apache.gravitino.cli.commands.RemoveRoleFromUser;
import org.apache.gravitino.cli.commands.UserAudit;
import org.apache.gravitino.cli.commands.UserDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestUserCommands {
  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
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
        .newListUsers(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
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
        .newUserDetails(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user");
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
        .newUserAudit(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "admin");
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
        .newCreateUser(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user");
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
        .newDeleteUser(GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo", "user");
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
        .newDeleteUser(GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo", "user");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user", "admin");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user", "admin");
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
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user", "admin");

    // Verify second role
    doReturn(mockRemoveSecondRole)
        .when(commandLine)
        .newRemoveRoleFromUser(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user", "role1");

    commandLine.handleCommandLine();

    verify(mockRemoveSecondRole).handle();
    verify(mockRemoveFirstRole).handle();
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
        .newAddRoleToUser(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user", "admin");

    // Verify second role
    doReturn(mockAddSecondRole)
        .when(commandLine)
        .newAddRoleToUser(
            GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "user", "role1");

    commandLine.handleCommandLine();

    verify(mockAddFirstRole).handle();
    verify(mockAddSecondRole).handle();
  }
}
