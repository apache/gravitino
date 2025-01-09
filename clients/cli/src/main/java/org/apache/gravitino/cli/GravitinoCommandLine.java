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
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
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
    if (CommandActions.HELP.equals(command)) {
      handleHelpCommand();
    } else if (line.hasOption(GravitinoOptions.OWNER)) {
      handleOwnerCommand();
    } else if (entity.equals(CommandEntities.COLUMN)) {
      handleColumnCommand();
    } else if (entity.equals(CommandEntities.TABLE)) {
      new TableCommandHandler(this, line, command, ignore).handle();
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
    } else if (entity.equals(CommandEntities.MODEL)) {
      handleModelCommand();
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
    String outputFormat = line.getOptionValue(GravitinoOptions.OUTPUT);

    Command.setAuthenticationMode(auth, userName);

    if (CommandActions.LIST.equals(command)) {
      newListMetalakes(url, ignore, outputFormat).validate().handle();
      return;
    }

    String metalake = name.getMetalakeName();

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newMetalakeAudit(url, ignore, metalake).validate().handle();
        } else {
          newMetalakeDetails(url, ignore, outputFormat, metalake).validate().handle();
        }
        break;

      case CommandActions.CREATE:
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newCreateMetalake(url, ignore, metalake, comment).validate().handle();
        break;

      case CommandActions.DELETE:
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newDeleteMetalake(url, ignore, force, metalake).validate().handle();
        break;

      case CommandActions.SET:
        String property = line.getOptionValue(GravitinoOptions.PROPERTY);
        String value = line.getOptionValue(GravitinoOptions.VALUE);
        newSetMetalakeProperty(url, ignore, metalake, property, value).validate().handle();
        break;

      case CommandActions.REMOVE:
        property = line.getOptionValue(GravitinoOptions.PROPERTY);
        newRemoveMetalakeProperty(url, ignore, metalake, property).validate().handle();
        break;

      case CommandActions.PROPERTIES:
        newListMetalakeProperties(url, ignore, metalake).validate().handle();
        break;

      case CommandActions.UPDATE:
        if (line.hasOption(GravitinoOptions.ENABLE) && line.hasOption(GravitinoOptions.DISABLE)) {
          System.err.println(ErrorMessages.INVALID_ENABLE_DISABLE);
          Main.exit(-1);
        }
        if (line.hasOption(GravitinoOptions.ENABLE)) {
          boolean enableAllCatalogs = line.hasOption(GravitinoOptions.ALL);
          newMetalakeEnable(url, ignore, metalake, enableAllCatalogs).validate().handle();
        }
        if (line.hasOption(GravitinoOptions.DISABLE)) {
          newMetalakeDisable(url, ignore, metalake).validate().handle();
        }

        if (line.hasOption(GravitinoOptions.COMMENT)) {
          comment = line.getOptionValue(GravitinoOptions.COMMENT);
          newUpdateMetalakeComment(url, ignore, metalake, comment).validate().handle();
        }
        if (line.hasOption(GravitinoOptions.RENAME)) {
          String newName = line.getOptionValue(GravitinoOptions.RENAME);
          force = line.hasOption(GravitinoOptions.FORCE);
          newUpdateMetalakeName(url, ignore, force, metalake, newName).validate().handle();
        }

        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
        Main.exit(-1);
        break;
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
    List<String> missingEntities = Lists.newArrayList();

    // Handle the CommandActions.LIST action separately as it doesn't use `catalog`
    if (CommandActions.LIST.equals(command)) {
      newListCatalogs(url, ignore, outputFormat, metalake).validate().handle();
      return;
    }

    String catalog = name.getCatalogName();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    checkEntities(missingEntities);

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newCatalogAudit(url, ignore, metalake, catalog).validate().handle();
        } else {
          newCatalogDetails(url, ignore, outputFormat, metalake, catalog).validate().handle();
        }
        break;

      case CommandActions.CREATE:
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        String provider = line.getOptionValue(GravitinoOptions.PROVIDER);
        String[] properties = line.getOptionValues(CommandActions.PROPERTIES);
        Map<String, String> propertyMap = new Properties().parse(properties);
        newCreateCatalog(url, ignore, metalake, catalog, provider, comment, propertyMap)
            .validate()
            .handle();
        break;

      case CommandActions.DELETE:
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newDeleteCatalog(url, ignore, force, metalake, catalog).validate().handle();
        break;

      case CommandActions.SET:
        String property = line.getOptionValue(GravitinoOptions.PROPERTY);
        String value = line.getOptionValue(GravitinoOptions.VALUE);
        newSetCatalogProperty(url, ignore, metalake, catalog, property, value).validate().handle();
        break;

      case CommandActions.REMOVE:
        property = line.getOptionValue(GravitinoOptions.PROPERTY);
        newRemoveCatalogProperty(url, ignore, metalake, catalog, property).validate().handle();
        break;

      case CommandActions.PROPERTIES:
        newListCatalogProperties(url, ignore, metalake, catalog).validate().handle();
        break;

      case CommandActions.UPDATE:
        if (line.hasOption(GravitinoOptions.ENABLE) && line.hasOption(GravitinoOptions.DISABLE)) {
          System.err.println(ErrorMessages.INVALID_ENABLE_DISABLE);
          Main.exit(-1);
        }
        if (line.hasOption(GravitinoOptions.ENABLE)) {
          boolean enableMetalake = line.hasOption(GravitinoOptions.ALL);
          newCatalogEnable(url, ignore, metalake, catalog, enableMetalake).validate().handle();
        }
        if (line.hasOption(GravitinoOptions.DISABLE)) {
          newCatalogDisable(url, ignore, metalake, catalog).validate().handle();
        }

        if (line.hasOption(GravitinoOptions.COMMENT)) {
          String updateComment = line.getOptionValue(GravitinoOptions.COMMENT);
          newUpdateCatalogComment(url, ignore, metalake, catalog, updateComment)
              .validate()
              .handle();
        }
        if (line.hasOption(GravitinoOptions.RENAME)) {
          String newName = line.getOptionValue(GravitinoOptions.RENAME);
          newUpdateCatalogName(url, ignore, metalake, catalog, newName).validate().handle();
        }
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
        Main.exit(-1);
        break;
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

    List<String> missingEntities = Lists.newArrayList();
    if (metalake == null) missingEntities.add(CommandEntities.METALAKE);
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);

    // Handle the CommandActions.LIST action separately as it doesn't use `schema`
    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      newListSchema(url, ignore, metalake, catalog).validate().handle();
      return;
    }

    String schema = name.getSchemaName();
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);
    checkEntities(missingEntities);

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newSchemaAudit(url, ignore, metalake, catalog, schema).validate().handle();
        } else {
          newSchemaDetails(url, ignore, metalake, catalog, schema).validate().handle();
        }
        break;

      case CommandActions.CREATE:
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        newCreateSchema(url, ignore, metalake, catalog, schema, comment).validate().handle();
        break;

      case CommandActions.DELETE:
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newDeleteSchema(url, ignore, force, metalake, catalog, schema).validate().handle();
        break;

      case CommandActions.SET:
        String property = line.getOptionValue(GravitinoOptions.PROPERTY);
        String value = line.getOptionValue(GravitinoOptions.VALUE);
        newSetSchemaProperty(url, ignore, metalake, catalog, schema, property, value)
            .validate()
            .handle();
        break;

      case CommandActions.REMOVE:
        property = line.getOptionValue(GravitinoOptions.PROPERTY);
        newRemoveSchemaProperty(url, ignore, metalake, catalog, schema, property)
            .validate()
            .handle();
        break;

      case CommandActions.PROPERTIES:
        newListSchemaProperties(url, ignore, metalake, catalog, schema).validate().handle();
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
        Main.exit(-1);
        break;
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

    if (user == null && !CommandActions.LIST.equals(command)) {
      System.err.println(ErrorMessages.MISSING_USER);
      Main.exit(-1);
    }

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newUserAudit(url, ignore, metalake, user).validate().handle();
        } else {
          newUserDetails(url, ignore, metalake, user).validate().handle();
        }
        break;

      case CommandActions.LIST:
        newListUsers(url, ignore, metalake).validate().handle();
        break;

      case CommandActions.CREATE:
        newCreateUser(url, ignore, metalake, user).validate().handle();
        break;

      case CommandActions.DELETE:
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newDeleteUser(url, ignore, force, metalake, user).validate().handle();
        break;

      case CommandActions.REVOKE:
        String[] revokeRoles = line.getOptionValues(GravitinoOptions.ROLE);
        for (String role : revokeRoles) {
          newRemoveRoleFromUser(url, ignore, metalake, user, role).validate().handle();
        }
        System.out.printf("Remove roles %s from user %s%n", COMMA_JOINER.join(revokeRoles), user);
        break;

      case CommandActions.GRANT:
        String[] grantRoles = line.getOptionValues(GravitinoOptions.ROLE);
        for (String role : grantRoles) {
          newAddRoleToUser(url, ignore, metalake, user, role).validate().handle();
        }
        System.out.printf("Grant roles %s to user %s%n", COMMA_JOINER.join(grantRoles), user);
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
        Main.exit(-1);
        break;
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

    if (group == null && !CommandActions.LIST.equals(command)) {
      System.err.println(ErrorMessages.MISSING_GROUP);
      Main.exit(-1);
    }

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newGroupAudit(url, ignore, metalake, group).validate().handle();
        } else {
          newGroupDetails(url, ignore, metalake, group).validate().handle();
        }
        break;

      case CommandActions.LIST:
        newListGroups(url, ignore, metalake).validate().handle();
        break;

      case CommandActions.CREATE:
        newCreateGroup(url, ignore, metalake, group).validate().handle();
        break;

      case CommandActions.DELETE:
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newDeleteGroup(url, ignore, force, metalake, group).validate().handle();
        break;

      case CommandActions.REVOKE:
        String[] revokeRoles = line.getOptionValues(GravitinoOptions.ROLE);
        for (String role : revokeRoles) {
          newRemoveRoleFromGroup(url, ignore, metalake, group, role).validate().handle();
        }
        System.out.printf("Remove roles %s from group %s%n", COMMA_JOINER.join(revokeRoles), group);
        break;

      case CommandActions.GRANT:
        String[] grantRoles = line.getOptionValues(GravitinoOptions.ROLE);
        for (String role : grantRoles) {
          newAddRoleToGroup(url, ignore, metalake, group, role).validate().handle();
        }
        System.out.printf("Grant roles %s to group %s%n", COMMA_JOINER.join(grantRoles), group);
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        Main.exit(-1);
        break;
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
        newCreateTags(url, ignore, metalake, tags, comment).validate().handle();
        break;

      case CommandActions.DELETE:
        boolean forceDelete = line.hasOption(GravitinoOptions.FORCE);
        newDeleteTag(url, ignore, forceDelete, metalake, tags).validate().handle();
        break;

      case CommandActions.SET:
        String propertySet = line.getOptionValue(GravitinoOptions.PROPERTY);
        String valueSet = line.getOptionValue(GravitinoOptions.VALUE);
        if (propertySet == null && valueSet == null) {
          newTagEntity(url, ignore, metalake, name, tags).validate().handle();
        } else {
          newSetTagProperty(url, ignore, metalake, getOneTag(tags), propertySet, valueSet)
              .validate()
              .handle();
        }
        break;

      case CommandActions.REMOVE:
        boolean isTag = line.hasOption(GravitinoOptions.TAG);
        if (!isTag) {
          boolean forceRemove = line.hasOption(GravitinoOptions.FORCE);
          newRemoveAllTags(url, ignore, metalake, name, forceRemove).validate().handle();
        } else {
          String propertyRemove = line.getOptionValue(GravitinoOptions.PROPERTY);
          if (propertyRemove != null) {
            newRemoveTagProperty(url, ignore, metalake, getOneTag(tags), propertyRemove)
                .validate()
                .handle();
          } else {
            newUntagEntity(url, ignore, metalake, name, tags).validate().handle();
          }
        }
        break;

      case CommandActions.PROPERTIES:
        newListTagProperties(url, ignore, metalake, getOneTag(tags)).validate().handle();
        break;

      case CommandActions.UPDATE:
        if (line.hasOption(GravitinoOptions.COMMENT)) {
          String updateComment = line.getOptionValue(GravitinoOptions.COMMENT);
          newUpdateTagComment(url, ignore, metalake, getOneTag(tags), updateComment)
              .validate()
              .handle();
        }
        if (line.hasOption(GravitinoOptions.RENAME)) {
          String newName = line.getOptionValue(GravitinoOptions.RENAME);
          newUpdateTagName(url, ignore, metalake, getOneTag(tags), newName).validate().handle();
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

  /** Handles the command execution for Roles based on command type and the command line options. */
  protected void handleRoleCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String[] privileges = line.getOptionValues(GravitinoOptions.PRIVILEGE);

    Command.setAuthenticationMode(auth, userName);

    String[] roles = line.getOptionValues(GravitinoOptions.ROLE);
    if (roles == null && !CommandActions.LIST.equals(command)) {
      System.err.println(ErrorMessages.MISSING_ROLE);
      Main.exit(-1);
    }

    if (roles != null) {
      roles = Arrays.stream(roles).distinct().toArray(String[]::new);
    }

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newRoleAudit(url, ignore, metalake, getOneRole(roles)).validate().handle();
        } else {
          newRoleDetails(url, ignore, metalake, getOneRole(roles)).validate().handle();
        }
        break;

      case CommandActions.LIST:
        newListRoles(url, ignore, metalake).validate().handle();
        break;

      case CommandActions.CREATE:
        newCreateRole(url, ignore, metalake, roles).validate().handle();
        break;

      case CommandActions.DELETE:
        boolean forceDelete = line.hasOption(GravitinoOptions.FORCE);
        newDeleteRole(url, ignore, forceDelete, metalake, roles).validate().handle();
        break;

      case CommandActions.GRANT:
        newGrantPrivilegesToRole(url, ignore, metalake, getOneRole(roles), name, privileges)
            .validate()
            .handle();
        break;

      case CommandActions.REVOKE:
        newRevokePrivilegesFromRole(url, ignore, metalake, getOneRole(roles), name, privileges)
            .validate()
            .handle();
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        Main.exit(-1);
        break;
    }
  }

  private String getOneRole(String[] roles) {
    if (roles == null || roles.length != 1) {
      System.err.println(ErrorMessages.MULTIPLE_ROLE_COMMAND_ERROR);
      Main.exit(-1);
    }

    return roles[0];
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

    Command.setAuthenticationMode(auth, userName);

    List<String> missingEntities = Lists.newArrayList();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);
    if (table == null) missingEntities.add(CommandEntities.TABLE);

    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      newListColumns(url, ignore, metalake, catalog, schema, table).validate().handle();
      return;
    }

    String column = name.getColumnName();
    if (column == null) missingEntities.add(CommandEntities.COLUMN);
    checkEntities(missingEntities);

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newColumnAudit(url, ignore, metalake, catalog, schema, table, column).validate().handle();
        } else {
          System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
          Main.exit(-1);
        }
        break;

      case CommandActions.CREATE:
        {
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
              .validate()
              .handle();
          break;
        }

      case CommandActions.DELETE:
        newDeleteColumn(url, ignore, metalake, catalog, schema, table, column).validate().handle();
        break;

      case CommandActions.UPDATE:
        {
          if (line.hasOption(GravitinoOptions.COMMENT)) {
            String comment = line.getOptionValue(GravitinoOptions.COMMENT);
            newUpdateColumnComment(url, ignore, metalake, catalog, schema, table, column, comment)
                .validate()
                .handle();
          }
          if (line.hasOption(GravitinoOptions.RENAME)) {
            String newName = line.getOptionValue(GravitinoOptions.RENAME);
            newUpdateColumnName(url, ignore, metalake, catalog, schema, table, column, newName)
                .validate()
                .handle();
          }
          if (line.hasOption(GravitinoOptions.DATATYPE)
              && !line.hasOption(GravitinoOptions.DEFAULT)) {
            String datatype = line.getOptionValue(GravitinoOptions.DATATYPE);
            newUpdateColumnDatatype(url, ignore, metalake, catalog, schema, table, column, datatype)
                .validate()
                .handle();
          }
          if (line.hasOption(GravitinoOptions.POSITION)) {
            String position = line.getOptionValue(GravitinoOptions.POSITION);
            newUpdateColumnPosition(url, ignore, metalake, catalog, schema, table, column, position)
                .validate()
                .handle();
          }
          if (line.hasOption(GravitinoOptions.NULL)) {
            boolean nullable = line.getOptionValue(GravitinoOptions.NULL).equals("true");
            newUpdateColumnNullability(
                    url, ignore, metalake, catalog, schema, table, column, nullable)
                .validate()
                .handle();
          }
          if (line.hasOption(GravitinoOptions.AUTO)) {
            boolean autoIncrement = line.getOptionValue(GravitinoOptions.AUTO).equals("true");
            newUpdateColumnAutoIncrement(
                    url, ignore, metalake, catalog, schema, table, column, autoIncrement)
                .validate()
                .handle();
          }
          if (line.hasOption(GravitinoOptions.DEFAULT)) {
            String defaultValue = line.getOptionValue(GravitinoOptions.DEFAULT);
            String dataType = line.getOptionValue(GravitinoOptions.DATATYPE);
            newUpdateColumnDefault(
                    url, ignore, metalake, catalog, schema, table, column, defaultValue, dataType)
                .validate()
                .handle();
          }
          break;
        }

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        Main.exit(-1);
        break;
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

    switch (command) {
      case CommandActions.DETAILS:
        newOwnerDetails(url, ignore, metalake, entityName, entity).handle();
        break;

      case CommandActions.SET:
        {
          String owner = line.getOptionValue(GravitinoOptions.USER);
          String group = line.getOptionValue(GravitinoOptions.GROUP);

          if (owner != null && group == null) {
            newSetOwner(url, ignore, metalake, entityName, entity, owner, false).handle();
          } else if (owner == null && group != null) {
            newSetOwner(url, ignore, metalake, entityName, entity, group, true).handle();
          } else {
            System.err.println(ErrorMessages.INVALID_SET_COMMAND);
          }
          break;
        }

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        break;
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

    Command.setAuthenticationMode(auth, userName);

    List<String> missingEntities = Lists.newArrayList();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);

    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      newListTopics(url, ignore, metalake, catalog, schema).validate().handle();
      return;
    }

    String topic = name.getTopicName();
    if (topic == null) missingEntities.add(CommandEntities.TOPIC);
    checkEntities(missingEntities);

    switch (command) {
      case CommandActions.DETAILS:
        newTopicDetails(url, ignore, metalake, catalog, schema, topic).validate().handle();
        break;

      case CommandActions.CREATE:
        {
          String comment = line.getOptionValue(GravitinoOptions.COMMENT);
          newCreateTopic(url, ignore, metalake, catalog, schema, topic, comment)
              .validate()
              .handle();
          break;
        }

      case CommandActions.DELETE:
        {
          boolean force = line.hasOption(GravitinoOptions.FORCE);
          newDeleteTopic(url, ignore, force, metalake, catalog, schema, topic).validate().handle();
          break;
        }

      case CommandActions.UPDATE:
        {
          if (line.hasOption(GravitinoOptions.COMMENT)) {
            String comment = line.getOptionValue(GravitinoOptions.COMMENT);
            newUpdateTopicComment(url, ignore, metalake, catalog, schema, topic, comment)
                .validate()
                .handle();
          }
          break;
        }

      case CommandActions.SET:
        {
          String property = line.getOptionValue(GravitinoOptions.PROPERTY);
          String value = line.getOptionValue(GravitinoOptions.VALUE);
          newSetTopicProperty(url, ignore, metalake, catalog, schema, topic, property, value)
              .validate()
              .handle();
          break;
        }

      case CommandActions.REMOVE:
        {
          String property = line.getOptionValue(GravitinoOptions.PROPERTY);
          newRemoveTopicProperty(url, ignore, metalake, catalog, schema, topic, property)
              .validate()
              .handle();
          break;
        }

      case CommandActions.PROPERTIES:
        newListTopicProperties(url, ignore, metalake, catalog, schema, topic).validate().handle();
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        break;
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

    Command.setAuthenticationMode(auth, userName);

    List<String> missingEntities = Lists.newArrayList();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);

    // Handle CommandActions.LIST action separately as it doesn't require the `fileset`
    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      newListFilesets(url, ignore, metalake, catalog, schema).validate().handle();
      return;
    }

    String fileset = name.getFilesetName();
    if (fileset == null) missingEntities.add(CommandEntities.FILESET);
    checkEntities(missingEntities);

    switch (command) {
      case CommandActions.DETAILS:
        newFilesetDetails(url, ignore, metalake, catalog, schema, fileset).validate().handle();
        break;

      case CommandActions.CREATE:
        {
          String comment = line.getOptionValue(GravitinoOptions.COMMENT);
          String[] properties = line.getOptionValues(CommandActions.PROPERTIES);
          Map<String, String> propertyMap = new Properties().parse(properties);
          newCreateFileset(url, ignore, metalake, catalog, schema, fileset, comment, propertyMap)
              .validate()
              .handle();
          break;
        }

      case CommandActions.DELETE:
        {
          boolean force = line.hasOption(GravitinoOptions.FORCE);
          newDeleteFileset(url, ignore, force, metalake, catalog, schema, fileset)
              .validate()
              .handle();
          break;
        }

      case CommandActions.SET:
        {
          String property = line.getOptionValue(GravitinoOptions.PROPERTY);
          String value = line.getOptionValue(GravitinoOptions.VALUE);
          newSetFilesetProperty(url, ignore, metalake, catalog, schema, fileset, property, value)
              .validate()
              .handle();
          break;
        }

      case CommandActions.REMOVE:
        {
          String property = line.getOptionValue(GravitinoOptions.PROPERTY);
          newRemoveFilesetProperty(url, ignore, metalake, catalog, schema, fileset, property)
              .validate()
              .handle();
          break;
        }

      case CommandActions.PROPERTIES:
        newListFilesetProperties(url, ignore, metalake, catalog, schema, fileset)
            .validate()
            .handle();
        break;

      case CommandActions.UPDATE:
        {
          if (line.hasOption(GravitinoOptions.COMMENT)) {
            String comment = line.getOptionValue(GravitinoOptions.COMMENT);
            newUpdateFilesetComment(url, ignore, metalake, catalog, schema, fileset, comment)
                .validate()
                .handle();
          }
          if (line.hasOption(GravitinoOptions.RENAME)) {
            String newName = line.getOptionValue(GravitinoOptions.RENAME);
            newUpdateFilesetName(url, ignore, metalake, catalog, schema, fileset, newName)
                .validate()
                .handle();
          }
          break;
        }

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        break;
    }
  }

  /**
   * Handles the command execution for Models based on command type and the command line options.
   */
  private void handleModelCommand() {
    String url = getUrl();
    String auth = getAuth();
    String userName = line.getOptionValue(GravitinoOptions.LOGIN);
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();

    Command.setAuthenticationMode(auth, userName);

    List<String> missingEntities = Lists.newArrayList();
    if (catalog == null) missingEntities.add(CommandEntities.CATALOG);
    if (schema == null) missingEntities.add(CommandEntities.SCHEMA);

    // Handle CommandActions.LIST action separately as it doesn't require the `model`
    if (CommandActions.LIST.equals(command)) {
      checkEntities(missingEntities);
      newListModel(url, ignore, metalake, catalog, schema).validate().handle();
      return;
    }

    String model = name.getModelName();
    if (model == null) missingEntities.add(CommandEntities.MODEL);
    checkEntities(missingEntities);

    switch (command) {
      case CommandActions.DETAILS:
        if (line.hasOption(GravitinoOptions.AUDIT)) {
          newModelAudit(url, ignore, metalake, catalog, schema, model).validate().handle();
        } else {
          newModelDetails(url, ignore, metalake, catalog, schema, model).validate().handle();
        }
        break;

      case CommandActions.DELETE:
        boolean force = line.hasOption(GravitinoOptions.FORCE);
        newDeleteModel(url, ignore, force, metalake, catalog, schema, model).validate().handle();
        break;

      case CommandActions.CREATE:
        String createComment = line.getOptionValue(GravitinoOptions.COMMENT);
        String[] createProperties = line.getOptionValues(GravitinoOptions.PROPERTIES);
        Map<String, String> createPropertyMap = new Properties().parse(createProperties);
        newCreateModel(
                url, ignore, metalake, catalog, schema, model, createComment, createPropertyMap)
            .validate()
            .handle();
        break;

      case CommandActions.UPDATE:
        String[] alias = line.getOptionValues(GravitinoOptions.ALIAS);
        String uri = line.getOptionValue(GravitinoOptions.URI);
        String linkComment = line.getOptionValue(GravitinoOptions.COMMENT);
        String[] linkProperties = line.getOptionValues(CommandActions.PROPERTIES);
        Map<String, String> linkPropertityMap = new Properties().parse(linkProperties);
        newLinkModel(
                url,
                ignore,
                metalake,
                catalog,
                schema,
                model,
                uri,
                alias,
                linkComment,
                linkPropertityMap)
            .validate()
            .handle();
        break;

      default:
        System.err.println(ErrorMessages.UNSUPPORTED_ACTION);
        break;
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
