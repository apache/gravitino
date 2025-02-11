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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/* Gravitino Command line */
public class GravitinoCommandLine extends TestableCommandLine {

  private final CommandLine line;
  private final Options options;
  private final String entity;
  private final String command;
  private boolean ignore = false;
  private String ignoreEnv;
  private boolean ignoreSet = false;

  public static final String CMD = "gcli"; // recommended name
  public static final String DEFAULT_URL = "http://localhost:8090";
  // This joiner is used to join multiple outputs to be displayed, e.g. roles or groups

  /**
   * Gravitino Command line.
   *
   * @param line Parsed command line object.
   * @param options Available options for the CLI.
   * @param entity The entity to apply the command to e.g. metalake, catalog, schema, table etc.
   * @param command The type of command to run i.e. list, details, update, delete, or create.
   */
  public GravitinoCommandLine(CommandLine line, Options options, String entity, String command) {
    this.line = line;
    this.options = options;
    this.entity = entity;
    this.command = command;
  }

  /** Handles the parsed command line arguments and executes the corresponding actions. */
  public void handleCommandLine() {
    GravitinoConfig config = new GravitinoConfig(null);

    /* Check if you should ignore client/version versions */
    if (line.hasOption(GravitinoOptions.IGNORE)) {
      ignore = true;
    } else {
      // Cache the ignore environment variable
      if (ignoreEnv == null && !ignoreSet) {
        ignoreEnv = System.getenv("GRAVITINO_IGNORE");
        ignore = ignoreEnv != null && ignoreEnv.equals("true");
        ignoreSet = true;
      }

      // Check if the ignore name is specified in the configuration file
      if (ignoreEnv == null) {
        if (config.fileExists()) {
          config.read();
          ignore = config.getIgnore();
        }
      }
    }

    executeCommand();
  }

  /** Handles the parsed command line arguments and executes the corresponding actions. */
  public void handleSimpleLine() {
    /* Display command usage. */
    if (line.hasOption(GravitinoOptions.HELP)) {
      displayHelp(options);
    } else {
      new SimpleCommandHandler(this, line, ignore).handle();
    }
  }

  /**
   * Displays the help message for the command line tool.
   *
   * @param options The command options.
   */
  public static void displayHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(CMD, options);
  }

  /** Executes the appropriate command based on the command type. */
  private void executeCommand() {
    boolean force = line.hasOption(GravitinoOptions.FORCE);
    String outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);
    CommandContext context = new CommandContext(null, ignore, force, outputFormat);

    if (CommandActions.HELP.equals(command)) {
      handleHelpCommand();
    } else if (line.hasOption(GravitinoOptions.OWNER)) {
      new OwnerCommandHandler(this, line, command, ignore, entity).handle();
    } else if (entity.equals(CommandEntities.COLUMN)) {
      new ColumnCommandHandler(this, line, command, ignore).handle();
    } else if (entity.equals(CommandEntities.TABLE)) {
      new TableCommandHandler(this, line, command, context).handle();
    } else if (entity.equals(CommandEntities.SCHEMA)) {
      new SchemaCommandHandler(this, line, command, context).handle();
    } else if (entity.equals(CommandEntities.CATALOG)) {
      new CatalogCommandHandler(this, line, command, ignore).handle();
    } else if (entity.equals(CommandEntities.METALAKE)) {
      new MetalakeCommandHandler(this, line, command, context).handle();
    } else if (entity.equals(CommandEntities.TOPIC)) {
      new TopicCommandHandler(this, line, command, context).handle();
    } else if (entity.equals(CommandEntities.FILESET)) {
      new FilesetCommandHandler(this, line, command, context).handle();
    } else if (entity.equals(CommandEntities.USER)) {
      new UserCommandHandler(this, line, command, ignore).handle();
    } else if (entity.equals(CommandEntities.GROUP)) {
      new GroupCommandHandler(this, line, command, ignore).handle();
    } else if (entity.equals(CommandEntities.TAG)) {
      new TagCommandHandler(this, line, command, ignore).handle();
    } else if (entity.equals(CommandEntities.ROLE)) {
      new RoleCommandHandler(this, line, command, ignore).handle();
    } else if (entity.equals(CommandEntities.MODEL)) {
      new ModelCommandHandler(this, line, command, context).handle();
    }
  }

  private void handleHelpCommand() {
    String helpFile = entity.toLowerCase() + "_help.txt";

    try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(helpFile);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      StringBuilder helpMessage = new StringBuilder();
      String helpLine;
      while ((helpLine = reader.readLine()) != null) {
        helpMessage.append(helpLine).append(System.lineSeparator());
      }
      System.out.print(helpMessage.toString());
    } catch (IOException e) {
      System.err.println(ErrorMessages.HELP_FAILED + e.getMessage());
      Main.exit(-1);
    }
  }
}
