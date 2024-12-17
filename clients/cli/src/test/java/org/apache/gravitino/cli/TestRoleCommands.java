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
import org.apache.gravitino.cli.commands.CreateRole;
import org.apache.gravitino.cli.commands.DeleteRole;
import org.apache.gravitino.cli.commands.ListRoles;
import org.apache.gravitino.cli.commands.RoleAudit;
import org.apache.gravitino.cli.commands.RoleDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRoleCommands {
  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
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
        .newListRoles(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo");
    commandLine.handleCommandLine();
    verify(mockList).handle();
  }

  @Test
  void testRoleDetailsCommand() {
    RoleDetails mockDetails = mock(RoleDetails.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DETAILS));
    doReturn(mockDetails)
        .when(commandLine)
        .newRoleDetails(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "admin");
    commandLine.handleCommandLine();
    verify(mockDetails).handle();
  }

  @Test
  void testRoleAuditCommand() {
    RoleAudit mockAudit = mock(RoleAudit.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("group");
    when(mockCommandLine.hasOption(GravitinoOptions.AUDIT)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DETAILS));
    doReturn(mockAudit)
        .when(commandLine)
        .newRoleAudit(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "group");
    commandLine.handleCommandLine();
    verify(mockAudit).handle();
  }

  @Test
  void testCreateRoleCommand() {
    CreateRole mockCreate = mock(CreateRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.CREATE));
    doReturn(mockCreate)
        .when(commandLine)
        .newCreateRole(GravitinoCommandLine.DEFAULT_URL, false, "metalake_demo", "admin");
    commandLine.handleCommandLine();
    verify(mockCreate).handle();
  }

  @Test
  void testDeleteRoleCommand() {
    DeleteRole mockDelete = mock(DeleteRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteRole(GravitinoCommandLine.DEFAULT_URL, false, false, "metalake_demo", "admin");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }

  @Test
  void testDeleteRoleForceCommand() {
    DeleteRole mockDelete = mock(DeleteRole.class);
    when(mockCommandLine.hasOption(GravitinoOptions.METALAKE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.METALAKE)).thenReturn("metalake_demo");
    when(mockCommandLine.hasOption(GravitinoOptions.ROLE)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.ROLE)).thenReturn("admin");
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(
            new GravitinoCommandLine(
                mockCommandLine, mockOptions, CommandEntities.ROLE, CommandActions.DELETE));
    doReturn(mockDelete)
        .when(commandLine)
        .newDeleteRole(GravitinoCommandLine.DEFAULT_URL, false, true, "metalake_demo", "admin");
    commandLine.handleCommandLine();
    verify(mockDelete).handle();
  }
}
