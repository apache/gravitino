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

/** Handles the command execution for Users based on command type and the command line options. */
public class UserCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private final FullName name;
  private final String metalake;
  private String user;

  /**
   * Constructs a {@link UserCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   */
  public UserCommandHandler(
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

    user = line.getOptionValue(GravitinoOptions.USER);

    if (user == null && !CommandActions.LIST.equals(command)) {
      System.err.println(ErrorMessages.MISSING_USER);
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

      case CommandActions.LIST:
        handleListCommand();
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

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    this.gravitinoCommandLine.newListUsers(context, metalake).validate().handle();
  }

  /** Handles the "DETAILS" command. */
  private void handleDetailsCommand() {
    if (line.hasOption(GravitinoOptions.AUDIT)) {
      this.gravitinoCommandLine.newUserAudit(context, metalake, user).validate().handle();
    } else {
      this.gravitinoCommandLine.newUserDetails(context, metalake, user).validate().handle();
    }
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    this.gravitinoCommandLine.newCreateUser(context, metalake, user).validate().handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    this.gravitinoCommandLine.newDeleteUser(context, metalake, user).validate().handle();
  }

  /** Handles the "REVOKE" command. */
  private void handleRevokeCommand() {
    boolean removeAll = line.hasOption(GravitinoOptions.ALL);
    if (removeAll) {
      gravitinoCommandLine
          .newRemoveAllRoles(context, metalake, user, CommandEntities.USER)
          .validate()
          .handle();
      System.out.printf("Removed all roles from user %s%n", user);
    } else {
      String[] revokeRoles = line.getOptionValues(GravitinoOptions.ROLE);
      for (String role : revokeRoles) {
        this.gravitinoCommandLine
            .newRemoveRoleFromUser(context, metalake, user, role)
            .validate()
            .handle();
      }
      System.out.printf("Removed roles %s from user %s%n", COMMA_JOINER.join(revokeRoles), user);
    }
  }

  /** Handles the "GRANT" command. */
  private void handleGrantCommand() {
    String[] grantRoles = line.getOptionValues(GravitinoOptions.ROLE);
    for (String role : grantRoles) {
      this.gravitinoCommandLine.newAddRoleToUser(context, metalake, user, role).validate().handle();
    }
    System.out.printf("Add roles %s to user %s%n", COMMA_JOINER.join(grantRoles), user);
  }
}
