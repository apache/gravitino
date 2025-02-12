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

/**
 * Handles the command execution for Metalakes based on command type and the command line options.
 */
public class MetalakeCommandHandler extends CommandHandler {

  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final CommandContext context;
  private final String command;
  private String metalake;

  /**
   * Constructs a MetalakeCommandHandler instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   */
  public MetalakeCommandHandler(
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
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    Command.setAuthenticationMode(getAuth(line), userName);

    if (CommandActions.LIST.equals(command)) {
      handleListCommand();
      return;
    }

    metalake = name.getMetalakeName();

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

      case CommandActions.SET:
        handleSetCommand();
        return true;

      case CommandActions.REMOVE:
        handleRemoveCommand();
        return true;

      case CommandActions.PROPERTIES:
        handlePropertiesCommand();
        return true;

      case CommandActions.UPDATE:
        handleUpdateCommand();
        return true;

      default:
        return false;
    }
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    gravitinoCommandLine.newListMetalakes(context).validate().handle();
  }

  /** Handles the "DETAILS" command. */
  private void handleDetailsCommand() {
    if (line.hasOption(GravitinoOptions.AUDIT)) {
      gravitinoCommandLine.newMetalakeAudit(context, metalake).validate().handle();
    } else {
      gravitinoCommandLine.newMetalakeDetails(context, metalake).validate().handle();
    }
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    String comment = line.getOptionValue(GravitinoOptions.COMMENT);
    gravitinoCommandLine.newCreateMetalake(context, metalake, comment).validate().handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    gravitinoCommandLine.newDeleteMetalake(context, metalake).validate().handle();
  }

  /** Handles the "SET" command. */
  private void handleSetCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    String value = line.getOptionValue(GravitinoOptions.VALUE);
    gravitinoCommandLine
        .newSetMetalakeProperty(context, metalake, property, value)
        .validate()
        .handle();
  }

  /** Handles the "REMOVE" command. */
  private void handleRemoveCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    gravitinoCommandLine.newRemoveMetalakeProperty(context, metalake, property).validate().handle();
  }

  /** Handles the "PROPERTIES" command. */
  private void handlePropertiesCommand() {
    gravitinoCommandLine.newListMetalakeProperties(context, metalake).validate().handle();
  }

  /** Handles the "UPDATE" command. */
  private void handleUpdateCommand() {
    if (line.hasOption(GravitinoOptions.ENABLE) && line.hasOption(GravitinoOptions.DISABLE)) {
      System.err.println(ErrorMessages.INVALID_ENABLE_DISABLE);
      Main.exit(-1);
    }
    if (line.hasOption(GravitinoOptions.ENABLE)) {
      boolean enableAllCatalogs = line.hasOption(GravitinoOptions.ALL);
      gravitinoCommandLine
          .newMetalakeEnable(context, metalake, enableAllCatalogs)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.DISABLE)) {
      gravitinoCommandLine.newMetalakeDisable(context, metalake).validate().handle();
    }

    if (line.hasOption(GravitinoOptions.COMMENT)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      gravitinoCommandLine.newUpdateMetalakeComment(context, metalake, comment).validate().handle();
    }
    if (line.hasOption(GravitinoOptions.RENAME)) {
      String newName = line.getOptionValue(GravitinoOptions.RENAME);
      gravitinoCommandLine.newUpdateMetalakeName(context, metalake, newName).validate().handle();
    }
  }
}
