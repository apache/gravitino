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

/** Handles the command execution for Columns based on command type and the command line options. */
public class ColumnCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private final FullName name;
  private final String metalake;
  private final String catalog;
  private final String schema;
  private final String table;
  private String column;

  /**
   * Constructs a {@link ColumnCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   */
  public ColumnCommandHandler(
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
    this.table = name.getTableName();
  }

  /** Handles the command execution logic based on the provided command. */
  @Override
  protected void handle() {
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(getAuth(line), userName);

    List<String> missingEntities = Lists.newArrayList();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);
    if (table == null) missingEntities.add(CommandEntities.TABLE);

    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      handleListCommand();
      return;
    }

    this.column = name.getColumnName();
    if (column == null) missingEntities.add(CommandEntities.COLUMN);
    checkEntities(missingEntities);

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
          .newColumnAudit(context, metalake, catalog, schema, table, column)
          .validate()
          .handle();
    } else {
      System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
      Main.exit(-1);
    }
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    String datatype = line.getOptionValue(GravitinoOptions.DATATYPE);
    String comment = line.getOptionValue(GravitinoOptions.COMMENT);
    String position = line.getOptionValue(GravitinoOptions.POSITION);
    boolean nullable =
        !line.hasOption(GravitinoOptions.NULL)
            || line.getOptionValue(GravitinoOptions.NULL).equals("true");
    boolean autoIncrement =
        line.hasOption(GravitinoOptions.AUTO)
            && line.getOptionValue(GravitinoOptions.AUTO).equals("true");
    String defaultValue = line.getOptionValue(GravitinoOptions.DEFAULT);

    gravitinoCommandLine
        .newAddColumn(
            context,
            metalake,
            catalog,
            schema,
            table,
            column,
            datatype,
            comment,
            position,
            nullable,
            autoIncrement,
            defaultValue)
        .validate()
        .handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    gravitinoCommandLine
        .newDeleteColumn(context, metalake, catalog, schema, table, column)
        .validate()
        .handle();
  }

  /** Handles the "UPDATE" command. */
  private void handleUpdateCommand() {
    if (line.hasOption(GravitinoOptions.COMMENT)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      gravitinoCommandLine
          .newUpdateColumnComment(context, metalake, catalog, schema, table, column, comment)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.RENAME)) {
      String newName = line.getOptionValue(GravitinoOptions.RENAME);
      gravitinoCommandLine
          .newUpdateColumnName(context, metalake, catalog, schema, table, column, newName)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.DATATYPE) && !line.hasOption(GravitinoOptions.DEFAULT)) {
      String datatype = line.getOptionValue(GravitinoOptions.DATATYPE);
      gravitinoCommandLine
          .newUpdateColumnDatatype(context, metalake, catalog, schema, table, column, datatype)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.POSITION)) {
      String position = line.getOptionValue(GravitinoOptions.POSITION);
      gravitinoCommandLine
          .newUpdateColumnPosition(context, metalake, catalog, schema, table, column, position)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.NULL)) {
      boolean nullable = line.getOptionValue(GravitinoOptions.NULL).equals("true");
      gravitinoCommandLine
          .newUpdateColumnNullability(context, metalake, catalog, schema, table, column, nullable)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.AUTO)) {
      boolean autoIncrement = line.getOptionValue(GravitinoOptions.AUTO).equals("true");
      gravitinoCommandLine
          .newUpdateColumnAutoIncrement(
              context, metalake, catalog, schema, table, column, autoIncrement)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.DEFAULT)) {
      String defaultValue = line.getOptionValue(GravitinoOptions.DEFAULT);
      String dataType = line.getOptionValue(GravitinoOptions.DATATYPE);
      gravitinoCommandLine
          .newUpdateColumnDefault(
              context, metalake, catalog, schema, table, column, defaultValue, dataType)
          .validate()
          .handle();
    }
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    gravitinoCommandLine
        .newListColumns(context, metalake, catalog, schema, table)
        .validate()
        .handle();
  }
}
