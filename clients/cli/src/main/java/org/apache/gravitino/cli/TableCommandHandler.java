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
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

/** Handles the command execution for Tables based on command type and the command line options. */
public class TableCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private final FullName name;
  private final String metalake;
  private final String catalog;
  private final String schema;
  private String table;

  /**
   * Constructs a {@link TableCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   */
  public TableCommandHandler(
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
    Command.setAuthenticationMode(getAuth(line), userName);

    List<String> missingEntities = Lists.newArrayList();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);

    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      handleListCommand();
      return;
    }

    this.table = name.getTableName();
    if (table == null) missingEntities.add(CommandEntities.TABLE);
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
      gravitinoCommandLine
          .newTableAudit(context, metalake, catalog, schema, table)
          .validate()
          .handle();
    } else if (line.hasOption(GravitinoOptions.INDEX)) {
      gravitinoCommandLine
          .newListIndexes(context, metalake, catalog, schema, table)
          .validate()
          .handle();
    } else if (line.hasOption(GravitinoOptions.DISTRIBUTION)) {
      gravitinoCommandLine
          .newTableDistribution(context, metalake, catalog, schema, table)
          .validate()
          .handle();
    } else if (line.hasOption(GravitinoOptions.PARTITION)) {
      gravitinoCommandLine
          .newTablePartition(context, metalake, catalog, schema, table)
          .validate()
          .handle();
    } else if (line.hasOption(GravitinoOptions.SORTORDER)) {
      gravitinoCommandLine
          .newTableSortOrder(context, metalake, catalog, schema, table)
          .validate()
          .handle();
    } else {
      gravitinoCommandLine
          .newTableDetails(context, metalake, catalog, schema, table)
          .validate()
          .handle();
    }
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    String columnFile = line.getOptionValue(GravitinoOptions.COLUMNFILE);
    String comment = line.getOptionValue(GravitinoOptions.COMMENT);
    gravitinoCommandLine
        .newCreateTable(context, metalake, catalog, schema, table, columnFile, comment)
        .validate()
        .handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    gravitinoCommandLine
        .newDeleteTable(context, metalake, catalog, schema, table)
        .validate()
        .handle();
  }

  /** Handles the "SET" command. */
  private void handleSetCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    String value = line.getOptionValue(GravitinoOptions.VALUE);
    gravitinoCommandLine
        .newSetTableProperty(context, metalake, catalog, schema, table, property, value)
        .validate()
        .handle();
  }

  /** Handles the "REMOVE" command. */
  private void handleRemoveCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    gravitinoCommandLine
        .newRemoveTableProperty(context, metalake, catalog, schema, table, property)
        .validate()
        .handle();
  }
  /** Handles the "PROPERTIES" command. */
  private void handlePropertiesCommand() {
    gravitinoCommandLine
        .newListTableProperties(context, metalake, catalog, schema, table)
        .validate()
        .handle();
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    gravitinoCommandLine.newListTables(context, metalake, catalog, schema).validate().handle();
  }

  /** Handles the "UPDATE" command. */
  private void handleUpdateCommand() {
    if (line.hasOption(GravitinoOptions.COMMENT)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      gravitinoCommandLine
          .newUpdateTableComment(context, metalake, catalog, schema, table, comment)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.RENAME)) {
      String newName = line.getOptionValue(GravitinoOptions.RENAME);
      gravitinoCommandLine
          .newUpdateTableName(context, metalake, catalog, schema, table, newName)
          .validate()
          .handle();
    }
  }
}
