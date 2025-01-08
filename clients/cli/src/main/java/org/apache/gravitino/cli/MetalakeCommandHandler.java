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
  private final String command;
  private final boolean ignore;
  private final String url;

  /**
   * Constructs a MetalakeCommandHandler instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param ignore Ignore server version mismatch.
   */
  public MetalakeCommandHandler(
      GravitinoCommandLine gravitinoCommandLine, CommandLine line, String command, boolean ignore) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.ignore = ignore;
    this.url = getUrl(line);
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

    String metalake = name.getMetalakeName();

    if (!executeCommand(metalake)) {
      System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
      Main.exit(-1);
    }
  }

  /**
   * Executes the specific command based on the command type.
   *
   * @param metalake the name of the metalake
   * @return true if the command is supported, false otherwise
   */
  private boolean executeCommand(String metalake) {
    switch (command) {
      case CommandActions.DETAILS:
        handleDetailsCommand(metalake);
        return true;

      case CommandActions.CREATE:
        handleCreateCommand(metalake);
        return true;

      case CommandActions.DELETE:
        handleDeleteCommand(metalake);
        return true;

      case CommandActions.SET:
        handleSetCommand(metalake);
        return true;

      case CommandActions.REMOVE:
        handleRemoveCommand(metalake);
        return true;

      case CommandActions.PROPERTIES:
        handlePropertiesCommand(metalake);
        return true;

      case CommandActions.UPDATE:
        handleUpdateCommand(metalake);
        return true;

      default:
        return false;
    }
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    String outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);
    gravitinoCommandLine.newListMetalakes(url, ignore, outputFormat).validate().handle();
  }

  /**
   * Handles the "DETAILS" command.
   *
   * @param metalake the name of the metalake
   */
  private void handleDetailsCommand(String metalake) {
    if (line.hasOption(GravitinoOptions.AUDIT)) {
      gravitinoCommandLine.newMetalakeAudit(url, ignore, metalake).validate().handle();
    } else {
      String outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);
      gravitinoCommandLine
          .newMetalakeDetails(url, ignore, outputFormat, metalake)
          .validate()
          .handle();
    }
  }

  /**
   * Handles the "CREATE" command.
   *
   * @param metalake the name of the metalake
   */
  private void handleCreateCommand(String metalake) {
    String comment = line.getOptionValue(GravitinoOptions.COMMENT);
    gravitinoCommandLine.newCreateMetalake(url, ignore, metalake, comment).validate().handle();
  }

  /**
   * Handles the "DELETE" command.
   *
   * @param metalake the name of the metalake
   */
  private void handleDeleteCommand(String metalake) {
    boolean force = line.hasOption(GravitinoOptions.FORCE);
    gravitinoCommandLine.newDeleteMetalake(url, ignore, force, metalake).validate().handle();
  }

  /**
   * Handles the "SET" command.
   *
   * @param metalake the name of the metalake
   */
  private void handleSetCommand(String metalake) {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    String value = line.getOptionValue(GravitinoOptions.VALUE);
    gravitinoCommandLine
        .newSetMetalakeProperty(url, ignore, metalake, property, value)
        .validate()
        .handle();
  }

  /**
   * Handles the "REMOVE" command.
   *
   * @param metalake the name of the metalake
   */
  private void handleRemoveCommand(String metalake) {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    gravitinoCommandLine
        .newRemoveMetalakeProperty(url, ignore, metalake, property)
        .validate()
        .handle();
  }

  /**
   * Handles the "PROPERTIES" command.
   *
   * @param metalake the name of the metalake
   */
  private void handlePropertiesCommand(String metalake) {
    gravitinoCommandLine.newListMetalakeProperties(url, ignore, metalake).validate().handle();
  }

  /**
   * Handles the "UPDATE" command.
   *
   * @param metalake the name of the metalake
   */
  private void handleUpdateCommand(String metalake) {
    if (line.hasOption(GravitinoOptions.ENABLE) && line.hasOption(GravitinoOptions.DISABLE)) {
      System.err.println(ErrorMessages.INVALID_ENABLE_DISABLE);
      Main.exit(-1);
    }
    if (line.hasOption(GravitinoOptions.ENABLE)) {
      boolean enableAllCatalogs = line.hasOption(GravitinoOptions.ALL);
      gravitinoCommandLine
          .newMetalakeEnable(url, ignore, metalake, enableAllCatalogs)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.DISABLE)) {
      gravitinoCommandLine.newMetalakeDisable(url, ignore, metalake).validate().handle();
    }

    if (line.hasOption(GravitinoOptions.COMMENT)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      gravitinoCommandLine
          .newUpdateMetalakeComment(url, ignore, metalake, comment)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.RENAME)) {
      String newName = line.getOptionValue(GravitinoOptions.RENAME);
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      gravitinoCommandLine
          .newUpdateMetalakeName(url, ignore, force, metalake, newName)
          .validate()
          .handle();
    }
  }
}
