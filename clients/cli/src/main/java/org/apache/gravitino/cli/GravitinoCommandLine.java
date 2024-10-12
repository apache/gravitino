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
import org.apache.gravitino.cli.commands.AllMetalakeDetails;
import org.apache.gravitino.cli.commands.CatalogDetails;
import org.apache.gravitino.cli.commands.ClientVersion;
import org.apache.gravitino.cli.commands.CreateHadoopCatalog;
import org.apache.gravitino.cli.commands.CreateHiveCatalog;
import org.apache.gravitino.cli.commands.CreateIcebergCatalog;
import org.apache.gravitino.cli.commands.CreateKafkaCatalog;
import org.apache.gravitino.cli.commands.CreateMetalake;
import org.apache.gravitino.cli.commands.CreateMySQLCatalog;
import org.apache.gravitino.cli.commands.CreatePostgresCatalog;
import org.apache.gravitino.cli.commands.CreateSchema;
import org.apache.gravitino.cli.commands.DeleteCatalog;
import org.apache.gravitino.cli.commands.DeleteMetalake;
import org.apache.gravitino.cli.commands.DeleteSchema;
import org.apache.gravitino.cli.commands.DeleteTable;
import org.apache.gravitino.cli.commands.ListCatalogProperties;
import org.apache.gravitino.cli.commands.ListCatalogs;
import org.apache.gravitino.cli.commands.ListColumns;
import org.apache.gravitino.cli.commands.ListMetalakeProperties;
import org.apache.gravitino.cli.commands.ListMetalakes;
import org.apache.gravitino.cli.commands.ListSchema;
import org.apache.gravitino.cli.commands.ListSchemaProperties;
import org.apache.gravitino.cli.commands.ListTables;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.RemoveSchemaProperty;
import org.apache.gravitino.cli.commands.SchemaDetails;
import org.apache.gravitino.cli.commands.ServerVersion;
import org.apache.gravitino.cli.commands.SetCatalogProperty;
import org.apache.gravitino.cli.commands.SetMetalakeProperty;
import org.apache.gravitino.cli.commands.SetSchemaProperty;
import org.apache.gravitino.cli.commands.TableDetails;
import org.apache.gravitino.cli.commands.UpdateCatalogComment;
import org.apache.gravitino.cli.commands.UpdateCatalogName;
import org.apache.gravitino.cli.commands.UpdateMetalakeComment;
import org.apache.gravitino.cli.commands.UpdateMetalakeName;

/* Gravitino Command line */
public class GravitinoCommandLine {

  private CommandLine line;
  private Options options;
  private String entity;
  private String command;

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
    /* Display command usage. */
    if (line.hasOption(GravitinoOptions.HELP)) {
      GravitinoCommandLine.displayHelp(options);
    }
    /* Display Gravitino version. */
    else if (line.hasOption(GravitinoOptions.VERSION)) {
      new ClientVersion(getUrl()).handle();
    } else if (line.hasOption(GravitinoOptions.SERVER)) {
      new ServerVersion(getUrl()).handle();
    } else {
      executeCommand();
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
    if (entity != null) {
      if (entity.equals(CommandEntities.TABLE)) {
        handleTableCommand();
      } else if (entity.equals(CommandEntities.SCHEMA)) {
        handleSchemaCommand();
      } else if (entity.equals(CommandEntities.CATALOG)) {
        handleCatalogCommand();
      } else if (entity.equals(CommandEntities.METALAKE)) {
        handleMetalakeCommand();
      }
    } else {
      handleGeneralCommand();
    }
  }

  /**
   * Handles the command execution for Metalakes based on command type and the command line options.
   */
  protected void handleMetalakeCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();

    if (CommandActions.DETAILS.equals(command)) {
      new MetalakeDetails(url, metalake).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListCatalogs(url, metalake).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      new CreateMetalake(url, metalake, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteMetalake(url, metalake).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      new SetMetalakeProperty(url, metalake, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      new RemoveMetalakeProperty(url, metalake, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      new ListMetalakeProperties(url, metalake).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        new UpdateMetalakeComment(url, metalake, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        new UpdateMetalakeName(url, metalake, newName).handle();
      }
    }
  }

  /**
   * Handles the command execution for Catalogs based on command type and the command line options.
   */
  protected void handleCatalogCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();

    if (CommandActions.DETAILS.equals(command)) {
      new CatalogDetails(url, metalake, catalog).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListSchema(url, metalake, catalog).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String provider = line.getOptionValue(GravitinoOptions.PROVIDER);
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      if (provider.equals(Providers.HIVE)) {
        String metastore = line.getOptionValue(GravitinoOptions.METASTORE);
        new CreateHiveCatalog(url, metalake, catalog, provider, comment, metastore).handle();
      } else if (provider.equals(Providers.ICEBERG)) {
        String metastore = line.getOptionValue(GravitinoOptions.METASTORE);
        String warehouse = line.getOptionValue(GravitinoOptions.WAREHOUSE);
        new CreateIcebergCatalog(url, metalake, catalog, provider, comment, metastore, warehouse)
            .handle();
      } else if (provider.equals(Providers.MYSQL)) {
        String jdbcurl = line.getOptionValue(GravitinoOptions.JDBCURL);
        String user = line.getOptionValue(GravitinoOptions.USER);
        String password = line.getOptionValue(GravitinoOptions.PASSWORD);
        new CreateMySQLCatalog(url, metalake, catalog, provider, comment, jdbcurl, user, password)
            .handle();
      } else if (provider.equals(Providers.POSTGRES)) {
        String jdbcurl = line.getOptionValue(GravitinoOptions.JDBCURL);
        String user = line.getOptionValue(GravitinoOptions.USER);
        String password = line.getOptionValue(GravitinoOptions.PASSWORD);
        String database = line.getOptionValue(GravitinoOptions.DATABASE);
        new CreatePostgresCatalog(
                url, metalake, catalog, provider, comment, jdbcurl, user, password, database)
            .handle();
      } else if (provider.equals(Providers.HADOOP)) {
        new CreateHadoopCatalog(url, metalake, catalog, provider, comment).handle();
      } else if (provider.equals(Providers.KAFKA)) {
        String bootstrap = line.getOptionValue(GravitinoOptions.BOOTSTRAP);
        new CreateKafkaCatalog(url, metalake, catalog, provider, comment, bootstrap).handle();
      }
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteCatalog(url, metalake, catalog).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      new SetCatalogProperty(url, metalake, catalog, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      new RemoveCatalogProperty(url, metalake, catalog, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      new ListCatalogProperties(url, metalake, catalog).handle();
    } else if (CommandActions.UPDATE.equals(command)) {
      if (line.hasOption(GravitinoOptions.COMMENT)) {
        String comment = line.getOptionValue(GravitinoOptions.COMMENT);
        new UpdateCatalogComment(url, metalake, catalog, comment).handle();
      }
      if (line.hasOption(GravitinoOptions.RENAME)) {
        String newName = line.getOptionValue(GravitinoOptions.RENAME);
        new UpdateCatalogName(url, metalake, catalog, newName).handle();
      }
    }
  }

  /**
   * Handles the command execution for Schemas based on command type and the command line options.
   */
  protected void handleSchemaCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();

    if (CommandActions.DETAILS.equals(command)) {
      new SchemaDetails(url, metalake, catalog, schema).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListTables(url, metalake, catalog, schema).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      String comment = line.getOptionValue(GravitinoOptions.COMMENT);
      new CreateSchema(url, metalake, catalog, schema, comment).handle();
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteSchema(url, metalake, catalog, schema).handle();
    } else if (CommandActions.SET.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      String value = line.getOptionValue(GravitinoOptions.VALUE);
      new SetSchemaProperty(url, metalake, catalog, schema, property, value).handle();
    } else if (CommandActions.REMOVE.equals(command)) {
      String property = line.getOptionValue(GravitinoOptions.PROPERTY);
      new RemoveSchemaProperty(url, metalake, catalog, schema, property).handle();
    } else if (CommandActions.PROPERTIES.equals(command)) {
      new ListSchemaProperties(url, metalake, catalog, schema).handle();
    }
  }

  /**
   * Handles the command execution for Tables based on command type and the command line options.
   */
  protected void handleTableCommand() {
    String url = getUrl();
    FullName name = new FullName(line);
    String metalake = name.getMetalakeName();
    String catalog = name.getCatalogName();
    String schema = name.getSchemaName();
    String table = name.getTableName();

    if (CommandActions.DETAILS.equals(command)) {
      new TableDetails(url, metalake, catalog, schema, table).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListColumns(url, metalake, catalog, schema, table).handle();
    } else if (CommandActions.CREATE.equals(command)) {
      // TODO
    } else if (CommandActions.DELETE.equals(command)) {
      new DeleteTable(url, metalake, catalog, schema, table).handle();
    }
  }

  /** Handles the command execution based on command type and the command line options. */
  protected void handleGeneralCommand() {
    String url = getUrl();

    if (CommandActions.DETAILS.equals(command)) {
      new AllMetalakeDetails(url).handle();
    } else if (CommandActions.LIST.equals(command)) {
      new ListMetalakes(url).handle();
    }
  }

  /**
   * Gets the Gravitino URL from the command line options, or returns the default URL.
   *
   * @return The Gravitino URL to be used.
   */
  protected String getUrl() {
    return line.hasOption(GravitinoOptions.URL)
        ? line.getOptionValue(GravitinoOptions.URL)
        : DEFAULT_URL;
  }
}
