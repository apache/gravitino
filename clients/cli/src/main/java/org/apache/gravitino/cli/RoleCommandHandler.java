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

import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

public class RoleCommandHandler extends CommandHandler {

  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private String metalake;
  private String[] roles;
  private String[] privileges;

  public RoleCommandHandler(
      GravitinoCommandLine gravitinoCommandLine,
      CommandLine line,
      String command,
      CommandContext context) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.context = context;
  }

  /** Handles the command execution logic based on the provided command. */
  public void handle() {
    String auth = getAuth(line);
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(auth, userName);

    metalake = new FullName(line).getMetalakeName();

    roles = line.getOptionValues(GravitinoOptions.ROLE);
    if (roles == null && !CommandActions.LIST.equals(command)) {
      System.err.println(ErrorMessages.MISSING_ROLE);
      Main.exit(-1);
    }
    if (roles != null) {
      roles = Arrays.stream(roles).distinct().toArray(String[]::new);
    }

    privileges = line.getOptionValues(GravitinoOptions.PRIVILEGE);

    if (!executeCommand()) {
      System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
      Main.exit(-1);
    }
  }

  /**
   * Executes the specific command based on the command type.
   *
   * @return true if the command is supported, false otherwise
   */
  private boolean executeCommand() {
    switch (command) {
      case CommandActions.DETAILS:
        handleDetailsCommand();
        return true;

      case CommandActions.LIST:
        handleListCommand();
        return true;

      case CommandActions.CREATE:
        handleCreateCommand();
        return true;

      case CommandActions.DELETE:
        handleDeleteCommand();
        return true;

      case CommandActions.GRANT:
        handleGrantCommand();
        return true;

      case CommandActions.REVOKE:
        handleRevokeCommand();
        return true;

      default:
        return false;
    }
  }

  private void handleDetailsCommand() {
    if (line.hasOption(GravitinoOptions.AUDIT)) {
      gravitinoCommandLine.newRoleAudit(context, metalake, getOneRole()).validate().handle();
    } else {
      gravitinoCommandLine.newRoleDetails(context, metalake, getOneRole()).validate().handle();
    }
  }

  private void handleListCommand() {
    gravitinoCommandLine.newListRoles(context, metalake).validate().handle();
  }

  private void handleCreateCommand() {
    gravitinoCommandLine.newCreateRole(context, metalake, roles).validate().handle();
  }

  private void handleDeleteCommand() {
    gravitinoCommandLine.newDeleteRole(context, metalake, roles).validate().handle();
  }

  private void handleGrantCommand() {
    gravitinoCommandLine
        .newGrantPrivilegesToRole(context, metalake, getOneRole(), new FullName(line), privileges)
        .validate()
        .handle();
  }

  private void handleRevokeCommand() {
    boolean removeAll = line.hasOption(GravitinoOptions.ALL);
    if (removeAll) {
      gravitinoCommandLine
          .newRevokeAllPrivileges(context, metalake, getOneRole(), new FullName(line))
          .validate()
          .handle();
    } else {
      gravitinoCommandLine
          .newRevokePrivilegesFromRole(
              context, metalake, getOneRole(), new FullName(line), privileges)
          .validate()
          .handle();
    }
  }

  private String getOneRole() {
    if (roles == null || roles.length != 1) {
      System.err.println(ErrorMessages.MULTIPLE_ROLE_COMMAND_ERROR);
      Main.exit(-1);
    }

    return roles[0];
  }
}
