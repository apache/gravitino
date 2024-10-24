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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.CatalogDetails;
import org.apache.gravitino.cli.commands.ClientVersion;
import org.apache.gravitino.cli.commands.ListCatalogs;
import org.apache.gravitino.cli.commands.ListColumns;
import org.apache.gravitino.cli.commands.ListMetalakes;
import org.apache.gravitino.cli.commands.ListSchema;
import org.apache.gravitino.cli.commands.ListTables;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.SchemaDetails;
import org.apache.gravitino.cli.commands.ServerVersion;
import org.apache.gravitino.cli.commands.TableDetails;

/* Gravitino Command line */
public class GravitinoCommandLine {

  private final CommandLine line;
  private final Options options;
  private final String entity;
  private final String command;
  private String urlEnv;
  private boolean urlSet = false;
  private boolean ignore = false;

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
    /* Check if you should ignore client/version versions */
    if (line.hasOption(GravitinoOptions.IGNORE)) {
      ignore = true;
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
      new MetalakeDetails(url, ignore, metalake).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListMetalakes(url, ignore).handle();
    }
  }

  /**
   * Handles the command execution for Catalogs based on command type and the command line options.
   */
  private void handleCatalogCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();

    if (CommandActions.DETAILS.equals(command)) {
      String catalog = name.getCatalogName();
      new CatalogDetails(url, ignore, metalake, catalog).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListCatalogs(url, ignore, metalake).handle();
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

    if (CommandActions.DETAILS.equals(command)) {
      String schema = name.getSchemaName();
      new SchemaDetails(url, ignore, metalake, catalog, schema).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListSchema(url, ignore, metalake, catalog).handle();
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

    if (CommandActions.DETAILS.equals(command)) {
      String table = name.getTableName();
      new TableDetails(url, ignore, metalake, catalog, schema, table).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListTables(url, ignore, metalake, catalog, schema).handle();
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

    if (CommandActions.LIST.equals(command)) {
      String table = name.getTableName();
      new ListColumns(url, ignore, metalake, catalog, schema, table).handle();
    }
  }

  /**
   * Retrieves the Gravitinno URL from the command line options or the GRAVITINO_URL environment
   * variable.
   *
   * @return The Gravitinno URL, or null if not found.
   */
  public String getUrl() {
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

    // Return the default localhost URL
    return DEFAULT_URL;
  }
}
