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

/**
 * Handles the command execution for Functions based on command type and the command line options.
 */
public class FunctionCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final String command;
  private final CommandContext context;
  private final FullName name;
  private final String metalake;
  private final String catalog;
  private final String schema;

  /**
   * Constructs a {@link FunctionCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param command The command to execute.
   * @param context The command context.
   */
  public FunctionCommandHandler(
      GravitinoCommandLine gravitinoCommandLine,
      CommandLine line,
      String command,
      CommandContext context) {
    super();
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.command = command;
    this.context = context;

    this.name = new FullName(line);
    this.metalake = name.getMetalakeName();
    this.catalog = name.getCatalogName();
    this.schema = name.getSchemaName();
  }

  @Override
  protected void handle() {
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    Command.setAuthenticationMode(context.auth(), userName);

    List<String> missingEntities = collectCommonMissingEntities();

    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      handleListCommand();
      return;
    }

    String functionName = name.getFunctionName();
    if (functionName == null) {
      missingEntities.add(CommandEntities.FUNCTION);
    }

    checkEntities(missingEntities);

    if (!executeCommand()) {
      System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
      Main.exit(-1);
    }
  }

  private boolean executeCommand() {
    return true;
  }

  private void handleListCommand() {
    gravitinoCommandLine.newListFunctions(context, metalake, name).validate().handle();
  }

  private List<String> collectCommonMissingEntities() {
    List<String> missingEntities = Lists.newArrayList();

    if (catalog == null) {
      missingEntities.add(CommandEntities.CATALOG);
    }
    if (schema == null) {
      missingEntities.add(CommandEntities.SCHEMA);
    }

    return missingEntities;
  }
}
