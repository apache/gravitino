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

import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.Command;

/* Gravitino Command line */
public class GravitinoCommandLine extends TestableCommandLine {

  private final CommandLine line;
  private final Options options;
  private final String entity;
  private final String command;
  private String urlEnv;
  private boolean urlSet = false;
  private boolean ignore = false;
  private String ignoreEnv;
  private boolean ignoreSet = false;
  private String authEnv;
  private boolean authSet = false;

  public static final String CMD = "gcli"; // recommended name
  public static final String DEFAULT_URL = "http://localhost:8090";
  // This joiner is used to join multiple outputs to be displayed, e.g. roles or groups
  private static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

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
    }
    /* Display Gravitino client version. */
    else if (line.hasOption(GravitinoOptions.VERSION)) {
      newClientVersion(getUrl(), ignore).handle();
    }
    /* Display Gravitino server version. */
    else if (line.hasOption(GravitinoOptions.SERVER)) {
      newServerVersion(getUrl(), ignore).handle();
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
    boolean quiet = line.hasOption(GravitinoOptions.QUIET);
    if (CommandActions.HELP.equals(command)) {
      handleHelpCommand();
    } else if (line.hasOption(GravitinoOptions.OWNER)) {
      new OwnerCommandHandler(this, line, command, ignore, quiet, entity).handle();
    } else if (entity.equals(CommandEntities.COLUMN)) {
      new ColumnCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.TABLE)) {
      new TableCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.SCHEMA)) {
      new SchemaCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.CATALOG)) {
      new CatalogCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.METALAKE)) {
      new MetalakeCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.TOPIC)) {
      new TopicCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.FILESET)) {
      new FilesetCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.USER)) {
      new UserCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.GROUP)) {
      new GroupCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.TAG)) {
      handleTagCommand();
    } else if (entity.equals(CommandEntities.ROLE)) {
      new RoleCommandHandler(this, line, command, ignore, quiet).handle();
    } else if (entity.equals(CommandEntities.MODEL)) {
      new ModelCommandHandler(this, line, command, ignore, quiet).handle();
    }
  }

  /** Handles the command execution for Tags based on command type and the command line options. */
  protected void handleTagCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    boolean quiet = line.hasOption(GravitinoOptions.QUIET);

    Command.setAuthenticationMode(auth, userName);

    String[] tags = line.getOptionValues(GravitinoOptions.TAG);

    if (tags != null) {
      tags = Arrays.stream(tags).distinct().toArray(String[]::new);
    }

    switch (command) {
      case CommandActions.DETAILS:
        newTagDetails(url, ignore, metalake, getOneTag(tags)).validate().handle();
        break;

      case CommandActions.LIST:
        if (!name.hasCatalogName()) {
          newListTags(url, ignore, metalake).validate().handle();
        } else {
          newListEntityTags(url, ignore, metalake, name).validate().handle();
        }
        break;

      case CommandActions.CREATE:
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newCreateTags(url, ignore, quiet, metalake, tags, comment).validate().handle();
        break;

      case CommandActions.DELETE:
        boolean forceDelete = line.hasOption(GravitinoOptions.FORCE);
        newDeleteTag(url, ignore, quiet, forceDelete, metalake, tags).validate().handle();
        break;

      case CommandActions.SET:
        String propertySet = line.getOptionValue(GravitinoOptions.PROPERTY);
        String valueSet = line.getOptionValue(GravitinoOptions.VALUE);
        if (propertySet == null && valueSet == null) {
          newTagEntity(url, ignore, quiet, metalake, name, tags).validate().handle();
        } else {
          newSetTagProperty(url, ignore, quiet, metalake, getOneTag(tags), propertySet, valueSet)
              .validate()
              .handle();
        }
        break;

      case CommandActions.REMOVE:
        boolean isTag = line.hasOption(GravitinoOptions.TAG);
        if (!isTag) {
          boolean forceRemove = line.hasOption(GravitinoOptions.FORCE);
          newRemoveAllTags(url, ignore, quiet, metalake, name, forceRemove).validate().handle();
        } else {
          String propertyRemove = line.getOptionValue(GravitinoOptions.PROPERTY);
          if (propertyRemove != null) {
            newRemoveTagProperty(url, ignore, quiet, metalake, getOneTag(tags), propertyRemove)
                .validate()
                .handle();
          } else {
            newUntagEntity(url, ignore, quiet, metalake, name, tags).validate().handle();
          }
        }
        break;

      case CommandActions.PROPERTIES:
        newListTagProperties(url, ignore, metalake, getOneTag(tags)).validate().handle();
        break;

      case CommandActions.UPDATE:
        if (line.hasOption(GravitinoOptions.COMMENT)) {
          String updateComment = line.getOptionValue(GravitinoOptions.COMMENT);
          newUpdateTagComment(url, ignore, quiet, metalake, getOneTag(tags), updateComment)
              .validate()
              .handle();
        }
        if (line.hasOption(GravitinoOptions.RENAME)) {
          String newName = line.getOptionValue(GravitinoOptions.RENAME);
          newUpdateTagName(url, ignore, quiet, metalake, getOneTag(tags), newName)
              .validate()
              .handle();
        }
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        Main.exit(-1);
        break;
    }
  }

  private String getOneTag(String[] tags) {
    if (tags == null || tags.length > 1) {
      System.err.println(ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR);
      Main.exit(-1);
    }
    return tags[0];
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

  /**
   * Retrieves the Gravitinno URL from the command line options or the GRAVITINO_URL environment
   * variable or the Gravitio config file.
   *
   * @return The Gravitinno URL, or null if not found.
   */
  public String getUrl() {
    GravitinoConfig config = new GravitinoConfig(null);

    // If specified on the command line use that
    if (line.hasOption(GravitinoOptions.URL)) {
      return line.getOptionValue(GravitinoOptions.URL);
    }

    // Cache the Gravitino URL environment variable
    if (urlEnv == null && !urlSet) {
      urlEnv = System.getenv("GRAVITINO_URL");
      urlSet = true;
    }

    // If set return the Gravitino URL environment variable
    if (urlEnv != null) {
      return urlEnv;
    }

    // Check if the Gravitino URL is specified in the configuration file
    if (config.fileExists()) {
      config.read();
      String configURL = config.getGravitinoURL();
      if (configURL != null) {
        return configURL;
      }
    }

    // Return the default localhost URL
    return DEFAULT_URL;
  }

  /**
   * Retrieves the Gravitinno authentication from the command line options or the GRAVITINO_AUTH
   * environment variable or the Gravitio config file.
   *
   * @return The Gravitinno authentication, or null if not found.
   */
  public String getAuth() {
    // If specified on the command line use that
    if (line.hasOption(GravitinoOptions.SIMPLE)) {
      return GravitinoOptions.SIMPLE;
    }

    // Cache the Gravitino authentication type environment variable
    if (authEnv == null && !authSet) {
      authEnv = System.getenv("GRAVITINO_AUTH");
      authSet = true;
    }

    // If set return the Gravitino authentication type environment variable
    if (authEnv != null) {
      return authEnv;
    }

    // Check if the authentication type is specified in the configuration file
    GravitinoConfig config = new GravitinoConfig(null);
    if (config.fileExists()) {
      config.read();
      String configAuthType = config.getGravitinoAuthType();
      if (configAuthType != null) {
        return configAuthType;
      }
    }

    return null;
  }

  private void checkEntities(List<String> entities) {
    if (!entities.isEmpty()) {
      System.err.println(ErrorMessages.MISSING_ENTITIES + COMMA_JOINER.join(entities));
      Main.exit(-1);
    }
  }
}
