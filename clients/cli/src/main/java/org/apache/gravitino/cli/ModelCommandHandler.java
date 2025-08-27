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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

/** Handles the command execution for Models based on command type and the command line options. */
public class ModelCommandHandler extends CommandHandler {
  private static final String DELIMITER = ",";
  private static final String KEY_VALUE_SEPARATOR = "=";

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

      case CommandActions.SET:
        handleSetCommand();
        return true;

      case CommandActions.REMOVE:
        handleRemoveCommand();
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
    if (line.hasOption(GravitinoOptions.URIS)) {
      String[] alias = line.getOptionValues(GravitinoOptions.ALIAS);
      Map<String, String> uris = getUrisFromLime(line);
      String linkComment = line.getOptionValue(GravitinoOptions.COMMENT);
      String[] linkProperties = line.getOptionValues(CommandActions.PROPERTIES);
      Map<String, String> linkPropertityMap = new Properties().parse(linkProperties);
      gravitinoCommandLine
          .newLinkModel(
              context,
              metalake,
              catalog,
              schema,
              model,
              uris,
              alias,
              linkComment,
              linkPropertityMap)
          .validate()
          .handle();
    }

    if (line.hasOption(GravitinoOptions.RENAME)) {
      String newName = line.getOptionValue(GravitinoOptions.RENAME);
      gravitinoCommandLine
          .newUpdateModelName(context, metalake, catalog, schema, model, newName)
          .validate()
          .handle();
    }

    if (line.hasOption(GravitinoOptions.COMMENT)
        && !(line.hasOption(GravitinoOptions.ALIAS) || line.hasOption(GravitinoOptions.VERSION))) {
      String newComment = line.getOptionValue(GravitinoOptions.COMMENT);
      gravitinoCommandLine
          .newUpdateModelComment(context, metalake, catalog, schema, model, newComment)
          .validate()
          .handle();
    }

    if (!line.hasOption(GravitinoOptions.URIS)
        && line.hasOption(GravitinoOptions.COMMENT)
        && (line.hasOption(GravitinoOptions.ALIAS) || line.hasOption(GravitinoOptions.VERSION))) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      Integer version = getVersionFromLine(line);
      String alias = getAliasFromLine(line);

      gravitinoCommandLine
          .newUpdateModelVersionComment(
              context, metalake, catalog, schema, model, version, alias, comment)
          .validate()
          .handle();
    }

    if (line.hasOption(GravitinoOptions.NEW_URI)
        && (line.hasOption(GravitinoOptions.ALIAS) || line.hasOption(GravitinoOptions.VERSION))) {
      String newUri = line.getOptionValue(GravitinoOptions.NEW_URI);
      Integer version = getVersionFromLine(line);
      String alias = getAliasFromLine(line);
      gravitinoCommandLine
          .newUpdateModelVersionUri(
              context, metalake, catalog, schema, model, version, alias, newUri)
          .validate()
          .handle();
    }

    if (line.hasOption(GravitinoOptions.NEW_ALIAS)
        && (line.hasOption(GravitinoOptions.ALIAS) || line.hasOption(GravitinoOptions.VERSION))) {
      String[] newAliases = line.getOptionValues(GravitinoOptions.NEW_ALIAS);
      Integer version = getVersionFromLine(line);
      String alias = getAliasFromLine(line);

      gravitinoCommandLine
          .newUpdateModelVersionAliases(
              context, metalake, catalog, schema, model, version, alias, newAliases, new String[0])
          .validate()
          .handle();
    }
  }

  /** Handles the "LIST" command. */
  private void handleListCommand() {
    gravitinoCommandLine.newListModel(context, metalake, catalog, schema).validate().handle();
  }

  /** Handles the "SET" command. */
  private void handleSetCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    String value = line.getOptionValue(GravitinoOptions.VALUE);

    if (line.hasOption(GravitinoOptions.ALIAS) || line.hasOption(GravitinoOptions.VERSION)) {
      Integer version = getVersionFromLine(line);
      String alias = getAliasFromLine(line);
      gravitinoCommandLine
          .newSetModelVersionProperty(
              context, metalake, catalog, schema, model, version, alias, property, value)
          .validate()
          .handle();
    } else {
      gravitinoCommandLine
          .newSetModelProperty(context, metalake, catalog, schema, model, property, value)
          .validate()
          .handle();
    }
  }

  /** Handles the "REMOVE" command. */
  private void handleRemoveCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);

    if (line.hasOption(GravitinoOptions.REMOVE_ALIAS)
        && (line.hasOption(GravitinoOptions.ALIAS) || line.hasOption(GravitinoOptions.VERSION))) {
      String[] removeAliases = line.getOptionValues(GravitinoOptions.REMOVE_ALIAS);
      Integer version = getVersionFromLine(line);
      String alias = getAliasFromLine(line);

      gravitinoCommandLine
          .newUpdateModelVersionAliases(
              context,
              metalake,
              catalog,
              schema,
              model,
              version,
              alias,
              new String[0],
              removeAliases)
          .validate()
          .handle();

    } else if (line.hasOption(GravitinoOptions.ALIAS) || line.hasOption(GravitinoOptions.VERSION)) {
      Integer version = getVersionFromLine(line);
      String alias = getAliasFromLine(line);

      gravitinoCommandLine
          .newRemoveModelVersionProperty(
              context, metalake, catalog, schema, model, version, alias, property)
          .validate()
          .handle();

    } else {
      gravitinoCommandLine
          .newRemoveModelProperty(context, metalake, catalog, schema, model, property)
          .validate()
          .handle();
    }
  }

  private String getOneAlias(String[] aliases) {
    if (aliases == null || aliases.length > 1) {
      System.err.println(ErrorMessages.MULTIPLE_ALIASES_COMMAND_ERROR);
      Main.exit(-1);
    }
    return aliases[0];
  }

  private Integer getVersionFromLine(CommandLine line) {
    return line.hasOption(GravitinoOptions.VERSION)
        ? Integer.parseInt(line.getOptionValue(GravitinoOptions.VERSION))
        : null;
  }

  private String getAliasFromLine(CommandLine line) {
    return line.hasOption(GravitinoOptions.ALIAS)
        ? getOneAlias(line.getOptionValues(GravitinoOptions.ALIAS))
        : null;
  }

  private Map<String, String> getUrisFromLime(CommandLine line) {
    String input = line.getOptionValue(GravitinoOptions.URIS);
    ImmutableMap.Builder<String, String> uris = ImmutableMap.builder();
    if (input != null) {
      String[] pairs = input.split(DELIMITER);
      for (String pair : pairs) {
        String[] keyValue = pair.split(KEY_VALUE_SEPARATOR, 2);
        if (keyValue.length == 2) {
          uris.put(keyValue[0].trim(), keyValue[1].trim());
        }
      }
    }
    return uris.build();
  }
}
