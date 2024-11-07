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

import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.CatalogAudit;
import org.apache.gravitino.cli.commands.CatalogDetails;
import org.apache.gravitino.cli.commands.ClientVersion;
import org.apache.gravitino.cli.commands.CreateCatalog;
import org.apache.gravitino.cli.commands.CreateGroup;
import org.apache.gravitino.cli.commands.CreateMetalake;
import org.apache.gravitino.cli.commands.CreateSchema;
import org.apache.gravitino.cli.commands.CreateUser;
import org.apache.gravitino.cli.commands.DeleteCatalog;
import org.apache.gravitino.cli.commands.DeleteGroup;
import org.apache.gravitino.cli.commands.DeleteMetalake;
import org.apache.gravitino.cli.commands.DeleteSchema;
import org.apache.gravitino.cli.commands.DeleteTable;
import org.apache.gravitino.cli.commands.DeleteUser;
import org.apache.gravitino.cli.commands.GroupDetails;
import org.apache.gravitino.cli.commands.ListCatalogProperties;
import org.apache.gravitino.cli.commands.ListCatalogs;
import org.apache.gravitino.cli.commands.ListColumns;
import org.apache.gravitino.cli.commands.ListGroups;
import org.apache.gravitino.cli.commands.ListMetalakeProperties;
import org.apache.gravitino.cli.commands.ListMetalakes;
import org.apache.gravitino.cli.commands.ListSchema;
import org.apache.gravitino.cli.commands.ListSchemaProperties;
import org.apache.gravitino.cli.commands.ListTables;
import org.apache.gravitino.cli.commands.ListUsers;
import org.apache.gravitino.cli.commands.MetalakeAudit;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.RemoveSchemaProperty;
import org.apache.gravitino.cli.commands.SchemaAudit;
import org.apache.gravitino.cli.commands.SchemaDetails;
import org.apache.gravitino.cli.commands.ServerVersion;
import org.apache.gravitino.cli.commands.SetCatalogProperty;
import org.apache.gravitino.cli.commands.SetMetalakeProperty;
import org.apache.gravitino.cli.commands.SetSchemaProperty;
import org.apache.gravitino.cli.commands.TableAudit;
import org.apache.gravitino.cli.commands.TableDetails;
import org.apache.gravitino.cli.commands.UpdateCatalogComment;
import org.apache.gravitino.cli.commands.UpdateCatalogName;
import org.apache.gravitino.cli.commands.UpdateMetalakeComment;
import org.apache.gravitino.cli.commands.UpdateMetalakeName;
import org.apache.gravitino.cli.commands.UserDetails;

/* Gravitino Command line */
public class GravitinoCommandLine {

  private final CommandLine line;
  private final Options options;
  private final String entity;
  private final String command;
  private String urlEnv;
  private boolean urlSet = false;
  private boolean ignore = false;
  private String ignoreEnv;
  private boolean ignoreSet = false;

  public static final String CMD = "gcli"; // recommended name
  public static final String DEFAULT_URL = "http://localhost:8090";

  /**
   * Gravitino Command line.
   *
   * @param line Parsed command line object.
   * @param options Available options for the CLI.
   * @param entity The entity to apply the command to e.g. metlake, catalog, schema, table etc etc.
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
      new ClientVersion(getUrl(), ignore).handle();
    }
    /* Display Gravitino server version. */
    else if (line.hasOption(GravitinoOptions.SERVER)) {
      new ServerVersion(getUrl(), ignore).handle();
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
    if (entity.equals(CommandEntities.COLUMN)) {
      handleColumnCommand();
    } else if (entity.equals(CommandEntities.TABLE)) {
      handleTableCommand();
    } else if (entity.equals(CommandEntities.SCHEMA)) {
      handleSchemaCommand();
    } else if (entity.equals(CommandEntities.CATALOG)) {
      handleCatalogCommand();
    } else if (entity.equals(CommandEntities.METALAKE)) {
      handleMetalakeCommand();
    } else if (entity.equals(CommandEntities.USER)) {
      handleUserCommand();
    } else if (entity.equals(CommandEntities.GROUP)) {
      handleGroupCommand();
    }
  }

  /**
   * Handles the command execution for Metalakes based on command type and the command line options.
   */
  private void handleMetalakeCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        new MetalakeAudit(url, ignore, metalake).handle();
      } else {
        new MetalakeDetails(url, ignore, metalake).handle();
      }
    } else if (CommandActions.LIST.equals(command)) {
      new ListMetalakes(url, ignore).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      new CreateMetalake(url, ignore, metalake, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteMetalake(url, ignore, metalake).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      new SetMetalakeProperty(url, ignore, metalake, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      new RemoveMetalakeProperty(url, ignore, metalake, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      new ListMetalakeProperties(url, ignore, metalake).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        new UpdateMetalakeComment(url, ignore, metalake, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        new UpdateMetalakeName(url, ignore, metalake, newName).handle();
      }
    }
  }

  /**
   * Handles the command execution for Catalogs based on command type and the command line options.
   */
  private void handleCatalogCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();

    if (CommandActions.LIST.equals(command)) {
      new ListCatalogs(url, ignore, metalake).handle();
      return;
    }

    String catalog = name.getCatalogName();

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        new CatalogAudit(url, ignore, metalake, catalog).handle();
      } else {
        new CatalogDetails(url, ignore, metalake, catalog).handle();
      }
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      String provider = line.getOptionValue(GravitinoOptions.PROVIDER);
      String[] properties = line.getOptionValues(GravitinoOptions.PROPERTIES);
      Map<String, String> propertyMap = new Properties().parse(properties);
      new CreateCatalog(url, ignore, metalake, catalog, provider, comment, propertyMap).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteCatalog(url, ignore, metalake, catalog).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      new SetCatalogProperty(url, ignore, metalake, catalog, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      new RemoveCatalogProperty(url, ignore, metalake, catalog, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      new ListCatalogProperties(url, ignore, metalake, catalog).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        new UpdateCatalogComment(url, ignore, metalake, catalog, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        new UpdateCatalogName(url, ignore, metalake, catalog, newName).handle();
      }
    }
  }

  /**
   * Handles the command execution for Schemas based on command type and the command line options.
   */
  private void handleSchemaCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();

    if (CommandActions.LIST.equals(command)) {
      new ListSchema(url, ignore, metalake, catalog).handle();
      return;
    }

    String schema = name.getSchemaName();

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        new SchemaAudit(url, ignore, metalake, catalog, schema).handle();
      } else {
        new SchemaDetails(url, ignore, metalake, catalog, schema).handle();
      }
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      new CreateSchema(url, ignore, metalake, catalog, schema, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteSchema(url, ignore, metalake, catalog, schema).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      new SetSchemaProperty(url, ignore, metalake, catalog, schema, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      new RemoveSchemaProperty(url, ignore, metalake, catalog, schema, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      new ListSchemaProperties(url, ignore, metalake, catalog, schema).handle();
    }
  }

  /**
   * Handles the command execution for Tables based on command type and the command line options.
   */
  private void handleTableCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();

    if (CommandActions.LIST.equals(command)) {
      new ListTables(url, ignore, metalake, catalog, schema).handle();
      return;
    }

    String table = name.getTableName();

    if (CommandActions.DETAILS.equals(command)) {
      if (line.hasOption(GravitinoOptions.AUDIT)) {
        new TableAudit(url, ignore, metalake, catalog, schema, table).handle();
      } else {
        new TableDetails(url, ignore, metalake, catalog, schema, table).handle();
      }
    } else if (CommandActions.CREATE.equals(command)) {
      // TODO
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteTable(url, ignore, metalake, catalog, schema, table).handle();
    }
  }

  /** Handles the command execution for Users based on command type and the command line options. */
  protected void handleUserCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String user = line.getOptionValue(GravitinoOptions.USER);

    if (CommandActions.DETAILS.equals(command)) {
      new UserDetails(url, ignore, metalake, user).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListUsers(url, ignore, metalake).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      new CreateUser(url, ignore, metalake, user).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteUser(url, ignore, metalake, user).handle();
    }
  }

  /** Handles the command execution for Group based on command type and the command line options. */
  protected void handleGroupCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String group = line.getOptionValue(GravitinoOptions.GROUP);

    if (CommandActions.DETAILS.equals(command)) {
      new GroupDetails(url, ignore, metalake, group).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListGroups(url, ignore, metalake).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      new CreateGroup(url, ignore, metalake, group).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteGroup(url, ignore, metalake, group).handle();
    }
  }

  /**
   * Handles the command execution for Columns based on command type and the command line options.
   */
  private void handleColumnCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();
    String table = name.getTableName();

    if (CommandActions.LIST.equals(command)) {
      new ListColumns(url, ignore, metalake, catalog, schema, table).handle();
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

    // Check if the metalake name is specified in the configuration file
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
}
