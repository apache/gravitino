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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

/**
 * Handles the command execution for Catalogs based on command type and the command line options.
 */
public class CatalogCommandHandler extends CommandHandler {

  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final boolean ignore;
  private final String url;
  private final FullName name;
  private final String metalake;
  private String catalog;
  private final String outputFormat;

  /**
   * Constructs a {@link CatalogCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param ignore Ignore server version mismatch.
   */
  public CatalogCommandHandler(
      GravitinoCommandLine gravitinoCommandLine, CommandLine line, String command, boolean ignore) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.ignore = ignore;

    this.url = getUrl(line);
    this.name = new FullName(line);
    this.metalake = name.getMetalakeName();
    this.outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);
  }

  /** Handles the command execution logic based on the provided command. */
  @Override
  protected void handle() {
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(getAuth(line), userName);
    List<String> missingEntities = Lists.newArrayList();

    if (CommandActions.LIST.equals(command)) {
      handleListCommand();
      return;
    }

    this.catalog = name.getCatalogName();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    checkEntities(missingEntities);

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

  /** Handles the "DETAILS" command. */
  private void handleDetailsCommand() {
    if (line.hasOption(GravitinoOptions.AUDIT)) {
      gravitinoCommandLine.newCatalogAudit(url, ignore, metalake, catalog).validate().handle();
    } else {
      gravitinoCommandLine
          .newCatalogDetails(url, ignore, outputFormat, metalake, catalog)
          .validate()
          .handle();
    }
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    String comment = line.getOptionValue(GravitinoOptions.COMMENT);
    String provider = line.getOptionValue(GravitinoOptions.PROVIDER);
    String[] properties = line.getOptionValues(CommandActions.PROPERTIES);

    Map<String, String> propertyMap = new Properties().parse(properties);
    gravitinoCommandLine
        .newCreateCatalog(url, ignore, metalake, catalog, provider, comment, propertyMap)
        .validate()
        .handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    boolean force = line.hasOption(GravitinoOptions.FORCE);
    gravitinoCommandLine
        .newDeleteCatalog(url, ignore, force, metalake, catalog)
        .validate()
        .handle();
  }

  /** Handles the "SET" command. */
  private void handleSetCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    String value = line.getOptionValue(GravitinoOptions.VALUE);
    gravitinoCommandLine
        .newSetCatalogProperty(url, ignore, metalake, catalog, property, value)
        .validate()
        .handle();
  }

  /** Handles the "REMOVE" command. */
  private void handleRemoveCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    gravitinoCommandLine
        .newRemoveCatalogProperty(url, ignore, metalake, catalog, property)
        .validate()
        .handle();
  }

  /** Handles the "PROPERTIES" command. */
  private void handlePropertiesCommand() {
    gravitinoCommandLine
        .newListCatalogProperties(url, ignore, metalake, catalog)
        .validate()
        .handle();
  }

  /** Handles the "UPDATE" command. */
  private void handleUpdateCommand() {
    if (line.hasOption(GravitinoOptions.ENABLE)) {
      boolean enableMetalake = line.hasOption(GravitinoOptions.ALL);
      boolean disable = line.hasOption(GravitinoOptions.DISABLE);
      gravitinoCommandLine
          .newCatalogEnable(url, ignore, metalake, catalog, disable, enableMetalake)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.DISABLE)) {
      gravitinoCommandLine.newCatalogDisable(url, ignore, metalake, catalog).validate().handle();
    }

    if (line.hasOption(GravitinoOptions.COMMENT)) {
      String updateComment = line.getOptionValue(GravitinoOptions.COMMENT);
      gravitinoCommandLine
          .newUpdateCatalogComment(url, ignore, metalake, catalog, updateComment)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.RENAME)) {
      String newName = line.getOptionValue(GravitinoOptions.RENAME);
      gravitinoCommandLine
          .newUpdateCatalogName(url, ignore, metalake, catalog, newName)
          .validate()
          .handle();
    }
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    gravitinoCommandLine.newListCatalogs(url, ignore, outputFormat, metalake).validate().handle();
  }
}
