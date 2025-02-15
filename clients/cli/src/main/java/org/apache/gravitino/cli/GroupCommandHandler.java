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

import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

/** Handles the command execution for Groups based on command type and the command line options. */
public class GroupCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private final FullName name;
  private final String metalake;
  private String group;

  /**
   * Constructs a {@link GroupCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   */
  public GroupCommandHandler(
      GravitinoCommandLine gravitinoCommandLine,
      CommandLine line,
      String command,
      CommandContext context) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.context = context;

    this.name = new FullName(line);
    this.metalake = name.getMetalakeName();
  }

  /** Handles the command execution logic based on the provided command. */
  @Override
  protected void handle() {
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(context.auth(), userName);

    if (CommandActions.LIST.equals(command)) {
      handleListCommand();
      return;
    }

    group = line.getOptionValue(GravitinoOptions.GROUP);
    if (group == null) {
      System.err.println(ErrorMessages.MISSING_GROUP);
      Main.exit(-1);
    }

    if (!executeCommand()) {
      System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
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

      case CommandActions.CREATE:
        handleCreateCommand();
        return true;

      case CommandActions.DELETE:
        handleDeleteCommand();
        return true;

      case CommandActions.REVOKE:
        handleRevokeCommand();
        return true;

      case CommandActions.GRANT:
        handleGrantCommand();
        return true;

      default:
        return false;
    }
  }

  /** Handles the "DETAILS" command. */
  private void handleDetailsCommand() {
    if (line.hasOption(GravitinoOptions.AUDIT)) {
      gravitinoCommandLine.newGroupAudit(context, metalake, group).validate().handle();
    } else {
      gravitinoCommandLine.newGroupDetails(context, metalake, group).validate().handle();
    }
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    gravitinoCommandLine.newCreateGroup(context, metalake, group).validate().handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    gravitinoCommandLine.newDeleteGroup(context, metalake, group).validate().handle();
  }

  /** Handles the "REVOKE" command. */
  private void handleRevokeCommand() {
    boolean revokeAll = line.hasOption(GravitinoOptions.ALL);
    if (revokeAll) {
      gravitinoCommandLine
          .newRemoveAllRoles(context, metalake, group, CommandEntities.GROUP)
          .validate()
          .handle();
      System.out.printf("Removed all roles from group %s%n", group);
    } else {
      String[] revokeRoles = line.getOptionValues(GravitinoOptions.ROLE);
      for (String role : revokeRoles) {
        gravitinoCommandLine
            .newRemoveRoleFromGroup(context, metalake, group, role)
            .validate()
            .handle();
      }
      System.out.printf("Removed roles %s from group %s%n", COMMA_JOINER.join(revokeRoles), group);
    }
  }

  /** Handles the "GRANT" command. */
  private void handleGrantCommand() {
    String[] grantRoles = line.getOptionValues(GravitinoOptions.ROLE);
    for (String role : grantRoles) {
      gravitinoCommandLine.newAddRoleToGroup(context, metalake, group, role).validate().handle();
    }
    System.out.printf("Grant roles %s to group %s%n", COMMA_JOINER.join(grantRoles), group);
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    gravitinoCommandLine.newListGroups(context, metalake).validate().handle();
  }
}
