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
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
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
    if (command.equals(CommandActions.HELP)) {
      handleHelpCommand();
    } else if (line.hasOption(GravitinoOptions.OWNER)) {
      handleOwnerCommand();
    } else if (entity.equals(CommandEntities.COLUMN)) {
      handleColumnCommand();
    } else if (entity.equals(CommandEntities.TABLE)) {
      handleTableCommand();
    } else if (entity.equals(CommandEntities.SCHEMA)) {
      handleSchemaCommand();
    } else if (entity.equals(CommandEntities.CATALOG)) {
      handleCatalogCommand();
    } else if (entity.equals(CommandEntities.METALAKE)) {
      handleMetalakeCommand();
    } else if (entity.equals(CommandEntities.TOPIC)) {
      handleTopicCommand();
    } else if (entity.equals(CommandEntities.FILESET)) {
      handleFilesetCommand();
    } else if (entity.equals(CommandEntities.USER)) {
      handleUserCommand();
    } else if (entity.equals(CommandEntities.GROUP)) {
      handleGroupCommand();
    } else if (entity.equals(CommandEntities.TAG)) {
      handleTagCommand();
    } else if (entity.equals(CommandEntities.ROLE)) {
      handleRoleCommand();
    }
  }

  /**
   * Handles the command execution for Metalakes based on command type and the command line options.
   */
  private void handleMetalakeCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        newMetalakeAudit(url, ignore, metalake).handle();
      } else {
        newMetalakeDetails(url, ignore, outputFormat, metalake).handle();
      }
    } else if (CommandActions.LIST.equals(command)) {
      newListMetalakes(url, ignore, outputFormat).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      newCreateMetalake(url, ignore, metalake, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteMetalake(url, ignore, force, metalake).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      newSetMetalakeProperty(url, ignore, metalake, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      newRemoveMetalakeProperty(url, ignore, metalake, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      newListMetalakeProperties(url, ignore, metalake).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newUpdateMetalakeComment(url, ignore, metalake, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newUpdateMetalakeName(url, ignore, force, metalake, newName).handle();
      }
    }
  }

  /**
   * Handles the command execution for Catalogs based on command type and the command line options.
   */
  private void handleCatalogCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.LIST.equals(command)) {
      newListCatalogs(url, ignore, metalake).handle();
      return;
    }

    String catalog = name.getCatalogName();

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        newCatalogAudit(url, ignore, metalake, catalog).handle();
      } else {
        newCatalogDetails(url, ignore, outputFormat, metalake, catalog).handle();
      }
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      String provider = line.getOptionValue(GravitinoOptions.PROVIDER);
      String[] properties = line.getOptionValues(GravitinoOptions.PROPERTIES);
      Map<String, String> propertyMap = new Properties().parse(properties);
      newCreateCatalog(url, ignore, metalake, catalog, provider, comment, propertyMap).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteCatalog(url, ignore, force, metalake, catalog).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      newSetCatalogProperty(url, ignore, metalake, catalog, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      newRemoveCatalogProperty(url, ignore, metalake, catalog, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      newListCatalogProperties(url, ignore, metalake, catalog).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newUpdateCatalogComment(url, ignore, metalake, catalog, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        newUpdateCatalogName(url, ignore, metalake, catalog, newName).handle();
      }
    }
  }

  /**
   * Handles the command execution for Schemas based on command type and the command line options.
   */
  private void handleSchemaCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.LIST.equals(command)) {
      newListSchema(url, ignore, metalake, catalog).handle();
      return;
    }

    String schema = name.getSchemaName();

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        newSchemaAudit(url, ignore, metalake, catalog, schema).handle();
      } else {
        newSchemaDetails(url, ignore, metalake, catalog, schema).handle();
      }
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      newCreateSchema(url, ignore, metalake, catalog, schema, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteSchema(url, ignore, force, metalake, catalog, schema).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      newSetSchemaProperty(url, ignore, metalake, catalog, schema, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      newRemoveSchemaProperty(url, ignore, metalake, catalog, schema, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      newListSchemaProperties(url, ignore, metalake, catalog, schema).handle();
    }
  }

  /**
   * Handles the command execution for Tables based on command type and the command line options.
   */
  private void handleTableCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.LIST.equals(command)) {
      newListTables(url, ignore, metalake, catalog, schema).handle();
      return;
    }

    String table = name.getTableName();

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        newTableAudit(url, ignore, metalake, catalog, schema, table).handle();
      } else if (line.hasOption(GravitinoOptions.INDEX)) {
        newListIndexes(url, ignore, metalake, catalog, schema, table).handle();
      } else if (line.hasOption(GravitinoOptions.DISTRIBUTION)) {
        newTableDistribution(url, ignore, metalake, catalog, schema, table).handle();
      } else if (line.hasOption(GravitinoOptions.PARTITION)) {
        newTablePartition(url, ignore, metalake, catalog, schema, table).handle();
      } else if (line.hasOption(GravitinoOptions.SORTORDER)) {
        newTableSortOrder(url, ignore, metalake, catalog, schema, table).handle();
      } else {
        newTableDetails(url, ignore, metalake, catalog, schema, table).handle();
      }
    } else if (CommandActions.CREATE.equals(command)) {
      String columnFile = line.getOptionValue(GravitinoOptions.COLUMNFILE);
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      newCreateTable(url, ignore, metalake, catalog, schema, table, columnFile, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteTable(url, ignore, force, metalake, catalog, schema, table).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      newSetTableProperty(url, ignore, metalake, catalog, schema, table, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      newRemoveTableProperty(url, ignore, metalake, catalog, schema, table, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      newListTableProperties(url, ignore, metalake, catalog, schema, table).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newUpdateTableComment(url, ignore, metalake, catalog, schema, table, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        newUpdateTableName(url, ignore, metalake, catalog, schema, table, newName).handle();
      }
    }
  }

  /** Handles the command execution for Users based on command type and the command line options. */
  protected void handleUserCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String user = line.getOptionValue(GravitinoOptions.USER);

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.DETAILS.equals(command)) {
      newUserDetails(url, ignore, metalake, user).handle();
    } else if (CommandActions.LIST.equals(command)) {
      newListUsers(url, ignore, metalake).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      newCreateUser(url, ignore, metalake, user).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteUser(url, ignore, force, metalake, user).handle();
    } else if (CommandActions.REVOKE.equals(command)) {
      String[] roles = line.getOptionValues(GravitinoOptions.ROLE);
      for (String role : roles) {
        newRemoveRoleFromUser(url, ignore, metalake, user, role).handle();
      }
      System.out.printf("Remove roles %s from user %s%n", COMMA_JOINER.join(roles), user);
    } else if (CommandActions.GRANT.equals(command)) {
      String[] roles = line.getOptionValues(GravitinoOptions.ROLE);
      for (String role : roles) {
        newAddRoleToUser(url, ignore, metalake, user, role).handle();
      }
      System.out.printf("Grant roles %s to user %s%n", String.join(", ", roles), user);
    } else {
      System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
    }
  }

  /** Handles the command execution for Group based on command type and the command line options. */
  protected void handleGroupCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String group = line.getOptionValue(GravitinoOptions.GROUP);

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.DETAILS.equals(command)) {
      newGroupDetails(url, ignore, metalake, group).handle();
    } else if (CommandActions.LIST.equals(command)) {
      newListGroups(url, ignore, metalake).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      newCreateGroup(url, ignore, metalake, group).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteGroup(url, ignore, force, metalake, group).handle();
    } else if (CommandActions.REVOKE.equals(command)) {
      String[] roles = line.getOptionValues(GravitinoOptions.ROLE);
      for (String role : roles) {
        newRemoveRoleFromGroup(url, ignore, metalake, group, role).handle();
      }
      System.out.printf("Remove roles %s from group %s%n", COMMA_JOINER.join(roles), group);
    } else if (CommandActions.GRANT.equals(command)) {
      String[] roles = line.getOptionValues(GravitinoOptions.ROLE);
      for (String role : roles) {
        newAddRoleToGroup(url, ignore, metalake, group, role).handle();
      }
      System.out.printf("Grant roles %s to group %s%n", COMMA_JOINER.join(roles), group);
    } else {
      System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
    }
  }

  /** Handles the command execution for Tags based on command type and the command line options. */
  protected void handleTagCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();

    Command.setAuthenticationMode(auth, userName);

    String[] tags = line.getOptionValues(GravitinoOptions.TAG);
    if (tags != null) {
      tags = Arrays.stream(tags).distinct().toArray(String[]::new);
    }
    if (CommandActions.DETAILS.equals(command)) {
      newTagDetails(url, ignore, metalake, getOneTag(tags)).handle();
    } else if (CommandActions.LIST.equals(command)) {
      if (!name.hasCatalogName()) {
        newListTags(url, ignore, metalake).handle();
      } else {
        newListEntityTags(url, ignore, metalake, name).handle();
      }
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      newCreateTags(url, ignore, metalake, tags, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteTag(url, ignore, force, metalake, tags).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      if (property != null && value != null) {
        newSetTagProperty(url, ignore, metalake, getOneTag(tags), property, value).handle();
      } else if (property == null && value == null) {
        newTagEntity(url, ignore, metalake, name, tags).handle();
      }
    } else if (CommandActions.REMOVE.equals(command)) {
      boolean isTag = line.hasOption(GravitinoOptions.TAG);
      if (!isTag) {
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newRemoveAllTags(url, ignore, metalake, name, force).handle();
      } else {
        String property = line.getOptionValue(GravitinoOptions.PROPERTY);
        if (property != null) {
          newRemoveTagProperty(url, ignore, metalake, getOneTag(tags), property).handle();
        } else {
          newUntagEntity(url, ignore, metalake, name, tags).handle();
        }
      }
    } else if (CommandActions.PROPERTIES.equals(command)) {
      newListTagProperties(url, ignore, metalake, getOneTag(tags)).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newUpdateTagComment(url, ignore, metalake, getOneTag(tags), comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        newUpdateTagName(url, ignore, metalake, getOneTag(tags), newName).handle();
      }
    }
  }

  private String getOneTag(String[] tags) {
    Preconditions.checkArgument(tags.length <= 1, ErrorMessages.MULTIPLE_TAG_COMMAND_ERROR);
    return tags[0];
  }

  /** Handles the command execution for Roles based on command type and the command line options. */
  protected void handleRoleCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String role = line.getOptionValue(GravitinoOptions.ROLE);

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.DETAILS.equals(command)) {
      newRoleDetails(url, ignore, metalake, role).handle();
    } else if (CommandActions.LIST.equals(command)) {
      newListRoles(url, ignore, metalake).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      newCreateRole(url, ignore, metalake, role).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteRole(url, ignore, force, metalake, role).handle();
    }
  }

  /**
   * Handles the command execution for Columns based on command type and the command line options.
   */
  private void handleColumnCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();
    String table = name.getTableName();
    String column = name.getColumnName();

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.LIST.equals(command)) {
      newListColumns(url, ignore, metalake, catalog, schema, table).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String datatype = line.getOptionValue(GravitinoOptions.DATATYPE);
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      String position = line.getOptionValue(GravitinoOptions.POSITION);
      boolean nullable = true;
      boolean autoIncrement = false;
      String defaultValue = line.getOptionValue(GravitinoOptions.DEFAULT);

      if (line.hasOption(GravitinoOptions.NULL)) {
        nullable = line.getOptionValue(GravitinoOptions.NULL).equals("true");
      }

      if (line.hasOption(GravitinoOptions.AUTO)) {
        autoIncrement = line.getOptionValue(GravitinoOptions.AUTO).equals("true");
      }

      newAddColumn(
              url,
              ignore,
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
          .handle();
    } else if (CommandActions.DELETE.equals(command)) {
      newDeleteColumn(url, ignore, metalake, catalog, schema, table, column).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newUpdateColumnComment(url, ignore, metalake, catalog, schema, table, column, comment)
            .handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        newUpdateColumnName(url, ignore, metalake, catalog, schema, table, column, newName)
            .handle();
      }
      if (line.hasOption(GravitinoOptions.DATATYPE)) {
        String datatype = line.getOptionValue(GravitinoOptions.DATATYPE);
        newUpdateColumnDatatype(url, ignore, metalake, catalog, schema, table, column, datatype)
            .handle();
      }
      if (line.hasOption(GravitinoOptions.POSITION)) {
        String position = line.getOptionValue(GravitinoOptions.POSITION);
        newUpdateColumnPosition(url, ignore, metalake, catalog, schema, table, column, position)
            .handle();
      }
      if (line.hasOption(GravitinoOptions.NULL)) {
        if (line.getOptionValue(GravitinoOptions.NULL).equals("true")) {
          newUpdateColumnNullability(url, ignore, metalake, catalog, schema, table, column, true)
              .handle();
        } else if (line.getOptionValue(GravitinoOptions.NULL).equals("false")) {
          newUpdateColumnNullability(url, ignore, metalake, catalog, schema, table, column, false)
              .handle();
        }
      }
      if (line.hasOption(GravitinoOptions.AUTO)) {
        if (line.getOptionValue(GravitinoOptions.AUTO).equals("true")) {
          newUpdateColumnAutoIncrement(url, ignore, metalake, catalog, schema, table, column, true)
              .handle();
        } else if (line.getOptionValue(GravitinoOptions.AUTO).equals("false")) {
          newUpdateColumnAutoIncrement(url, ignore, metalake, catalog, schema, table, column, false)
              .handle();
        }
      }
      if (line.hasOption(GravitinoOptions.DEFAULT)) {
        String defaultValue = line.getOptionValue(GravitinoOptions.DEFAULT);
        String dataType = line.getOptionValue(GravitinoOptions.DATATYPE);
        newUpdateColumnDefault(
                url, ignore, metalake, catalog, schema, table, column, defaultValue, dataType)
            .handle();
      }
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
      System.err.print(helpMessage.toString());
    } catch (IOException e) {
      System.err.println("Failed to load help message: " + e.getMessage());
    }
  }

  /**
   * Handles the command execution for Objects based on command type and the command line options.
   */
  private void handleOwnerCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String entityName = line.getOptionValue(GravitinoOptions.NAME);

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.DETAILS.equals(command)) {
      newOwnerDetails(url, ignore, metalake, entityName, entity).handle();
    } else if (CommandActions.SET.equals(command)) {
      String owner = line.getOptionValue(GravitinoOptions.USER);
      String group = line.getOptionValue(GravitinoOptions.GROUP);

      if (owner != null && group == null) {
        newSetOwner(url, ignore, metalake, entityName, entity, owner, false).handle();
      } else if (owner == null && group != null) {
        newSetOwner(url, ignore, metalake, entityName, entity, group, true).handle();
      } else {
        System.err.println(ErrorMessages.INVALID_SET_COMMAND);
      }
    } else {
      System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
    }
  }

  /**
   * Handles the command execution for topics based on command type and the command line options.
   */
  private void handleTopicCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();
    String topic = name.getTopicName();

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.LIST.equals(command)) {
      newListTopics(url, ignore, metalake, catalog, schema).handle();
    } else if (CommandActions.DETAILS.equals(command)) {
      newTopicDetails(url, ignore, metalake, catalog, schema, topic).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      newCreateTopic(url, ignore, metalake, catalog, schema, topic, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteTopic(url, ignore, force, metalake, catalog, schema, topic).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newUpdateTopicComment(url, ignore, metalake, catalog, schema, topic, comment).handle();
      }
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      newSetTopicProperty(url, ignore, metalake, catalog, schema, topic, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      newRemoveTopicProperty(url, ignore, metalake, catalog, schema, topic, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      newListTopicProperties(url, ignore, metalake, catalog, schema, topic).handle();
    }
  }

  /**
   * Handles the command execution for filesets based on command type and the command line options.
   */
  private void handleFilesetCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();
    String fileset = name.getFilesetName();

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.DETAILS.equals(command)) {
      newFilesetDetails(url, ignore, metalake, catalog, schema, fileset).handle();
    } else if (CommandActions.LIST.equals(command)) {
      newListFilesets(url, ignore, metalake, catalog, schema).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      String[] properties = line.getOptionValues(GravitinoOptions.PROPERTIES);
      Map<String, String> propertyMap = new Properties().parse(properties);
      newCreateFileset(url, ignore, metalake, catalog, schema, fileset, comment, propertyMap)
          .handle();
    } else if (CommandActions.DELETE.equals(command)) {
      boolean force = line.hasOption(GravitinoOptions.FORCE);
      newDeleteFileset(url, ignore, force, metalake, catalog, schema, fileset).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      newSetFilesetProperty(url, ignore, metalake, catalog, schema, fileset, property, value)
          .handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      newRemoveFilesetProperty(url, ignore, metalake, catalog, schema, fileset, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      newListFilesetProperties(url, ignore, metalake, catalog, schema, fileset).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newUpdateFilesetComment(url, ignore, metalake, catalog, schema, fileset, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        newUpdateFilesetName(url, ignore, metalake, catalog, schema, fileset, newName).handle();
      }
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
}
