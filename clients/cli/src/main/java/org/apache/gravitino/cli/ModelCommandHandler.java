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

/** Handles the command execution for Models based on command type and the command line options. */
public class ModelCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private final FullName name;
  private final String metalake;
  private final String catalog;
  private final String schema;
  private String model;

  /**
   * Constructs a {@link ModelCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   */
  public ModelCommandHandler(
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
    this.catalog = name.getCatalogName();
    this.schema = name.getSchemaName();
  }

  /** Handles the command execution logic based on the provided command. */
  @Override
  protected void handle() {
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(context.auth(), userName);

    List<String> missingEntities = Lists.newArrayList();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);

    // Handle CommandActions.LIST action separately as it doesn't require the `model`
    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      handleListCommand();
      return;
    }

    this.model = name.getModelName();
    if (model == null) missingEntities.add(CommandEntities.MODEL);
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
      gravitinoCommandLine
          .newModelAudit(context, metalake, catalog, schema, model)
          .validate()
          .handle();
    } else {
      gravitinoCommandLine
          .newModelDetails(context, metalake, catalog, schema, model)
          .validate()
          .handle();
    }
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    String createComment = line.getOptionValue(GravitinoOptions.COMMENT);
    String[] createProperties = line.getOptionValues(GravitinoOptions.PROPERTIES);
    Map<String, String> createPropertyMap = new Properties().parse(createProperties);
    gravitinoCommandLine
        .newCreateModel(context, metalake, catalog, schema, model, createComment, createPropertyMap)
        .validate()
        .handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    gravitinoCommandLine
        .newDeleteModel(context, metalake, catalog, schema, model)
        .validate()
        .handle();
  }

  /** Handles the "UPDATE" command. */
  private void handleUpdateCommand() {
    String[] alias = line.getOptionValues(GravitinoOptions.ALIAS);
    String uri = line.getOptionValue(GravitinoOptions.URI);
    String linkComment = line.getOptionValue(GravitinoOptions.COMMENT);
    String[] linkProperties = line.getOptionValues(CommandActions.PROPERTIES);
    Map<String, String> linkPropertityMap = new Properties().parse(linkProperties);
    gravitinoCommandLine
        .newLinkModel(
            context, metalake, catalog, schema, model, uri, alias, linkComment, linkPropertityMap)
        .validate()
        .handle();
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    gravitinoCommandLine.newListModel(context, metalake, catalog, schema).validate().handle();
  }
}
