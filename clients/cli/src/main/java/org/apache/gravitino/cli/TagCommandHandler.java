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

import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

public class TagCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private String[] tags;
  private String metalake;

  public TagCommandHandler(
      GravitinoCommandLine gravitinoCommandLine,
      CommandLine line,
      String command,
      CommandContext context) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.context = context;
    this.tags = line.getOptionValues(GravitinoOptions.TAG);

    if (tags != null) {
      tags = Arrays.stream(tags).distinct().toArray(String[]::new);
    }
  }

  @Override
  public void handle() {
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    Command.setAuthenticationMode(getAuth(line), userName);

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

      case CommandActions.LIST:
        handleListCommand();
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
    FullName name = new FullName(line);
    if (!name.hasCatalogName()) {
      gravitinoCommandLine.newListTags(context, metalake).validate().handle();
    } else {
      gravitinoCommandLine.newListEntityTags(context, metalake, name).validate().handle();
    }
  }

  /** Handles the "DETAILS" command. */
  private void handleDetailsCommand() {
    gravitinoCommandLine.newTagDetails(context, metalake, getOneTag(tags)).validate().handle();
  }

  /** Handles the "CREATE" command. */
  private void handleCreateCommand() {
    String comment = line.getOptionValue(GravitinoOptions.COMMENT);
    gravitinoCommandLine.newCreateTags(context, metalake, tags, comment).validate().handle();
  }

  /** Handles the "DELETE" command. */
  private void handleDeleteCommand() {
    gravitinoCommandLine.newDeleteTag(context, metalake, tags).validate().handle();
  }

  /** Handles the "SET" command. */
  private void handleSetCommand() {
    String property = line.getOptionValue(GravitinoOptions.PROPERTY);
    String value = line.getOptionValue(GravitinoOptions.VALUE);
    if (property == null && value == null) {
      gravitinoCommandLine
          .newTagEntity(context, metalake, new FullName(line), tags)
          .validate()
          .handle();
    } else {
      gravitinoCommandLine
          .newSetTagProperty(context, metalake, getOneTag(tags), property, value)
          .validate()
          .handle();
    }
  }

  /** Handles the "REMOVE" command. */
  private void handleRemoveCommand() {
    boolean isTag = line.hasOption(GravitinoOptions.TAG);
    FullName name = new FullName(line);
    if (!isTag) {
      gravitinoCommandLine.newRemoveAllTags(context, metalake, name).validate().handle();
    } else {
      String propertyRemove = line.getOptionValue(GravitinoOptions.PROPERTY);
      if (propertyRemove != null) {
        gravitinoCommandLine
            .newRemoveTagProperty(context, metalake, getOneTag(tags), propertyRemove)
            .validate()
            .handle();
      } else {
        gravitinoCommandLine.newUntagEntity(context, metalake, name, tags).validate().handle();
      }
    }
  }

  /** Handles the "PROPERTIES" command. */
  private void handlePropertiesCommand() {
    gravitinoCommandLine
        .newListTagProperties(context, metalake, getOneTag(tags))
        .validate()
        .handle();
  }

  /** Handles the "UPDATE" command. */
  private void handleUpdateCommand() {

    if (line.hasOption(GravitinoOptions.COMMENT)) {
      String updateComment = line.getOptionValue(GravitinoOptions.COMMENT);
      gravitinoCommandLine
          .newUpdateTagComment(context, metalake, getOneTag(tags), updateComment)
          .validate()
          .handle();
    }
    if (line.hasOption(GravitinoOptions.RENAME)) {
      String newName = line.getOptionValue(GravitinoOptions.RENAME);
      gravitinoCommandLine
          .newUpdateTagName(context, metalake, getOneTag(tags), newName)
          .validate()
          .handle();
    }
  }

  private String getOneTag(String[] tags) {
    if (tags == null || tags.length > 1) {
      System.err.println(ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR);
      Main.exit(-1);
    }
    return tags[0];
  }
}
