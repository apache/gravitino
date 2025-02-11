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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.gravitino.cli.commands.CreateRole;
import org.apache.gravitino.cli.commands.DeleteRole;
import org.apache.gravitino.cli.commands.GrantPrivilegesToRole;
import org.apache.gravitino.cli.commands.ListRoles;
import org.apache.gravitino.cli.commands.RevokeAllPrivileges;
import org.apache.gravitino.cli.commands.RevokePrivilegesFromRole;
import org.apache.gravitino.cli.commands.RoleAudit;
import org.apache.gravitino.cli.commands.RoleDetails;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRoleCommands {
  private CommandLine mockCommandLine;
  private Options options;
  private Options mockOptions;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
    options = new GravitinoOptions().options();
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testListRolesCommand() {
    ListRoles mockList = mock(ListRoles.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(CommandEntities.METALAKE)).thenReturn("metalake_demo");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.LIST));
    doReturn(mockList)
        .when(commandLine)
        .newListRoles(any(CommandContext.class), eq("metalake_demo"));
    doReturn(mockList).when(mockList).validate();
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testRoleDetailsCommand() {
    RoleDetails mockDetails = mock(RoleDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"admin"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newRoleDetails(any(CommandContext.class), eq("metalake_demo"), eq("admin"));

    doReturn(mockDetails).when(mockDetails).validate();
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testRoleDetailsCommandWithMultipleRoles() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE))
        .thenReturn(new String[] {"admin", "roleA", "roleB"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DETAILS));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newRoleDetails(any(CommandContext.class), eq("metalake_demo"), any());
  }

  @Test
  void testRoleAuditCommand() {
    RoleAudit mockAudit = mock(RoleAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"group"});
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newRoleAudit(any(CommandContext.class), eq("metalake_demo"), eq("group"));
    doReturn(mockAudit).when(mockAudit).validate();
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testCreateRoleCommand() {
    CreateRole mockCreate = mock(CreateRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"admin"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateRole(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(strings -> strings.length == 1 && "admin".equals(strings[0])));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testCreateRolesCommand() {
    CreateRole mockCreate = mock(CreateRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE))
        .thenReturn(new String[] {"admin", "engineer", "scientist"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.CREATE));

    doReturn(mockCreate)
        .when(commandLine)
        .newCreateRole(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(strings -> strings.length == 3));
    doReturn(mockCreate).when(mockCreate).validate();
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteRoleCommand() {
    DeleteRole mockDelete = mock(DeleteRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"admin"});
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteRole(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(strings -> strings.length == 1 && "admin".equals(strings[0])));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteRolesCommand() {
    DeleteRole mockDelete = mock(DeleteRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE))
        .thenReturn(new String[] {"admin", "engineer", "scientist"});
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DELETE));

    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteRole(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(strings -> strings.length == 3));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteRoleForceCommand() {
    DeleteRole mockDelete = mock(DeleteRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"admin"});
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteRole(
            any(CommandContext.class),
            eq("metalake_demo"),
            argThat(strings -> strings.length == 1 && "admin".equals(strings[0])));
    doReturn(mockDelete).when(mockDelete).validate();
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testGrantPrivilegesToRole() {
    GrantPrivilegesToRole mockGrant = mock(GrantPrivilegesToRole.class);
    String[] privileges = {"create_table", "modify_table"};
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"admin"});
    when(mockCommandLine.hasOption(GravitinoOptions.PRIVILEGE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.PRIVILEGE)).thenReturn(privileges);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.GRANT));
    doReturn(mockGrant)
        .when(commandLine)
        .newGrantPrivilegesToRole(
            any(CommandContext.class), eq("metalake_demo"), eq("admin"), any(), eq(privileges));
    doReturn(mockGrant).when(mockGrant).validate();
    commandLine.handleCommandLine();
    verify(mockGrant).handle();
  }

  @Test
  void testGrantPrivilegesToRoleWithoutPrivileges() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    GrantPrivilegesToRole spyGrantRole =
        spy(new GrantPrivilegesToRole(mockContext, "metalake_demo", "admin", null, null));
    assertThrows(RuntimeException.class, spyGrantRole::validate);
    verify(spyGrantRole, never()).handle();
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PRIVILEGES, errOutput);
  }

  @Test
  void testRevokePrivilegesFromRole() {
    RevokePrivilegesFromRole mockRevoke = mock(RevokePrivilegesFromRole.class);
    String[] privileges = {"create_table", "modify_table"};
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"admin"});
    when(mockCommandLine.hasOption(GravitinoOptions.PRIVILEGE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.PRIVILEGE)).thenReturn(privileges);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.REVOKE));
    doReturn(mockRevoke)
        .when(commandLine)
        .newRevokePrivilegesFromRole(
            any(CommandContext.class), eq("metalake_demo"), eq("admin"), any(), eq(privileges));
    doReturn(mockRevoke).when(mockRevoke).validate();
    commandLine.handleCommandLine();
    verify(mockRevoke).handle();
  }

  @Test
  void testRevokeAllPrivilegesFromRole() {
    RevokeAllPrivileges mockRevoke = mock(RevokeAllPrivileges.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.NAME)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.NAME)).thenReturn("catalog");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValues(GravitinoOptions.ROLE)).thenReturn(new String[] {"admin"});
    when(mockCommandLine.hasOption(GravitinoOptions.ALL)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.REVOKE));
    doReturn(mockRevoke)
        .when(commandLine)
        .newRevokeAllPrivileges(any(CommandContext.class), eq("metalake_demo"), eq("admin"), any());
    doReturn(mockRevoke).when(mockRevoke).validate();
    commandLine.handleCommandLine();
    verify(mockRevoke).handle();
  }

  @Test
  void testRevokeAllPrivilegesFromRoleWithoutName() throws ParseException {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    String[] args = {"role", "revoke", "-m", "metalake_demo", "--role", "admin", "--all"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);

    RevokeAllPrivileges spyRevokeAllPrivileges =
        spy(new RevokeAllPrivileges(mockContext, "metalake_demo", "admin", fullName));

    assertThrows(RuntimeException.class, spyRevokeAllPrivileges::validate);
    verify(spyRevokeAllPrivileges, never()).handle();
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_NAME, errOutput);
  }

  @Test
  void testRevokePrivilegesFromRoleWithoutPrivileges() {
    Main.useExit = false;
    CommandContext mockContext = mock(CommandContext.class);
    when(mockContext.url()).thenReturn(GravitinoCommandLine.DEFAULT_URL);
    RevokePrivilegesFromRole spyGrantRole =
        spy(new RevokePrivilegesFromRole(mockContext, "metalake_demo", "admin", null, null));
    assertThrows(RuntimeException.class, spyGrantRole::validate);
    verify(spyGrantRole, never()).handle();
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(ErrorMessages.MISSING_PRIVILEGES, errOutput);
  }

  @Test
  void testDeleteRoleCommandWithoutRole() {
    Main.useExit = false;
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(false);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.REVOKE));

    assertThrows(RuntimeException.class, commandLine::handleCommandLine);
    verify(commandLine, never())
        .newDeleteRole(any(CommandContext.class), eq("metalake_demo"), any());
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MISSING_ROLE);
  }
}
