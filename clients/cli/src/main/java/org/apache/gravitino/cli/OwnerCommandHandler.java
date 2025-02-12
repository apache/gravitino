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

/** Handles the command execution for Owner based on command type and the command line options. */
public class OwnerCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private final FullName name;
  private final String metalake;
  private final String entityName;
  private final String owner;
  private final String group;
  private final String entity;

  /**
   * Constructs a {@link OwnerCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   * @param entity The entity to execute the command on.
   */
  public OwnerCommandHandler(
      GravitinoCommandLine gravitinoCommandLine,
      CommandLine line,
      String command,
      CommandContext context,
      String entity) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.context = context;

    this.owner = line.getOptionValue(GravitinoOptions.USER);
    this.group = line.getOptionValue(GravitinoOptions.GROUP);
    this.name = new FullName(line);
    this.metalake = name.getMetalakeName();
    this.entityName = name.getName();
    this.entity = entity;
  }
  /** Handles the command execution logic based on the provided command. */
  @Override
  protected void handle() {
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(getAuth(line), userName);

    if (entityName == null && !CommandEntities.METALAKE.equals(entity)) {
      System.err.println(ErrorMessages.MISSING_NAME);
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

      case CommandActions.SET:
        handleSetCommand();
        return true;

      default:
        return false;
    }
  }

  /** Handles the "DETAILS" command. */
  private void handleDetailsCommand() {
    gravitinoCommandLine.newOwnerDetails(context, metalake, entityName, entity).validate().handle();
  }

  /** Handles the "SET" command. */
  private void handleSetCommand() {
    if (owner != null && group == null) {
      gravitinoCommandLine
          .newSetOwner(context, metalake, entityName, entity, owner, false)
          .validate()
          .handle();
    } else if (owner == null && group != null) {
      gravitinoCommandLine
          .newSetOwner(context, metalake, entityName, entity, group, true)
          .validate()
          .handle();
    } else {
      System.err.println(ErrorMessages.INVALID_SET_COMMAND);
      Main.exit(-1);
    }
  }
}
