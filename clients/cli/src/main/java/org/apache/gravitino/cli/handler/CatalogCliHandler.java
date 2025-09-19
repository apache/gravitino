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

package org.apache.gravitino.cli.handler;

import static org.apache.gravitino.cli.handler.CatalogCliHandler.CATALOG_VALIDATOR;
import static org.apache.gravitino.cli.handler.CatalogCliHandler.NO_OP_VALIDATOR;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.options.EnableDisableOptions;
import org.apache.gravitino.cli.options.PropertyOptions;
import picocli.CommandLine;

/** Handler for the catalog command. */
@CommandLine.Command(
    name = CommandEntities.CATALOG,
    description = "Operations on catalogs",
    subcommands = {
      CatalogDetails.class,
      CatalogCreate.class,
      CatalogDelete.class,
      CatalogSet.class,
      CatalogList.class,
      CatalogRemove.class,
      CatalogUpdate.class,
      CatalogProperties.class
    })
public class CatalogCliHandler implements Runnable {
  /** Validator of a Catalog Name option */
  public static final NameValidator CATALOG_VALIDATOR = new CatalogNameValidator();
  /** Validator doing nothing */
  public static final NameValidator NO_OP_VALIDATOR = fullName -> Lists.newArrayList();
  /** Joiner with , */
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

  /** Help option, use -h/--help */
  @CommandLine.Option(
      names = {"-h", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP},
      usageHelp = true,
      description = "display help message")
  boolean usageHelpRequested;

  /** Valid actions for the model command. */
  private static final String VALID_ACTIONS =
      COMMA_JOINER.join(
          new String[] {
            CommandActions.CREATE,
            CommandActions.DETAILS,
            CommandActions.UPDATE,
            CommandActions.LIST,
            CommandActions.DELETE,
            CommandActions.SET,
            CommandActions.PROPERTIES,
            CommandActions.REMOVE
          });

  /** {@inheritDoc} */
  @Override
  public void run() {
    System.out.println(CommandEntities.CATALOG + " " + "[command] [options]");
    System.out.println("Available catalog commands: " + VALID_ACTIONS);
  }
}

/** Handler for the catalog details command. */
@CommandLine.Command(
    name = CommandActions.DETAILS,
    description = "Get details of a catalog or get audit information for a catalog")
class CatalogDetails extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /**
   * Wether to show audit information for the catalog, use --audit to enable or --no-audit to
   * disable, default is false.
   */
  @CommandLine.Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.AUDIT,
      description = "Show audit information for the model",
      negatable = true,
      defaultValue = "false")
  boolean audit;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    return null;
  }

  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }
}

/** Handler for the catalog create command. */
@CommandLine.Command(name = CommandActions.CREATE, description = "Create a new catalog")
class CatalogCreate extends CliHandler {

  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROVIDER},
      description = "Provider for the catalog",
      required = true)
  Provider provider;

  @CommandLine.Option(
      names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
      description = "Comment for the Catalog",
      defaultValue = "")
  String comment;

  @CommandLine.Option(
      names = {"-p", "--properties"},
      description = "Properties in key=value format",
      split = ",")
  Map<String, String> properties;

  // TODO Merge with other Providers.
  enum Provider {
    /** Represents the Hive provider. */
    HIVE,

    /** Represents the Hadoop provider. */
    HADOOP,

    /** Represents the Iceberg provider. */
    ICEBERG,

    /** Represents the MySQL provider. */
    MYSQL,

    /** Represents the Postgres provider. */
    POSTGRES,

    /** Represents the Kafka provider. */
    KAFKA,

    /** Represents the Doris provider. */
    DORIS,

    /** Represents the Paimon provider. */
    PAIMON,

    /** Represents the Hudi provider. */
    HUDI,

    /** Represents the OceanBase provider. */
    OCEANBASE,

    /** Represents the Model provider. */
    MODEL;
  }

  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }

  /** {@inheritDoc} */
  @Override
  public Integer call() throws Exception {
    if (properties == null) {
      properties = Maps.newHashMap();
    }

    return super.call();
  }

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Handler for the catalog update command. */
@CommandLine.Command(name = CommandActions.DELETE, description = "Delete a catalog")
class CatalogDelete extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /**
   * Whether force to delete the catalog, use -f or --force to force delete the catalog, default is
   * false.
   */
  @CommandLine.Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.FORCE,
      description = "Whether force to delete the catalog",
      defaultValue = "false")
  boolean force;

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Handler for set command. */
@CommandLine.Command(name = CommandActions.SET, description = "Set property of a catalog")
class CatalogSet extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.ArgGroup(exclusive = false)
  PropertyOptions propertyOptions;

  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Handler for the catalog remove command. */
@CommandLine.Command(name = CommandActions.REMOVE, description = "Remove a property from a catalog")
class CatalogRemove extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY},
      required = true,
      description = "The property to remove from the catalog")
  String property;

  @Override
  protected Integer doCall() throws Exception {
    // TODO implement command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }
}

/** Handler for the catalog properties command. */
@CommandLine.Command(name = CommandActions.PROPERTIES, description = "List properties of a catalog")
class CatalogProperties extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }
}

/** Handler for the catalog list command. */
@CommandLine.Command(name = CommandActions.LIST, description = "List all catalogs in the metalake")
class CatalogList extends CliHandler {
  /** name of the entity, use --name/-n to specify the name */
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      description = "name of the entity")
  String name;

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the catalog update command. */
@CommandLine.Command(name = CommandActions.UPDATE, description = "Update a catalog")
class CatalogUpdate extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.ArgGroup(multiplicity = "1")
  UpdateOptions updateOptions;

  static class UpdateOptions {
    @CommandLine.ArgGroup(multiplicity = "1")
    EnableDisableOptions enableDisableOptions;

    @CommandLine.Option(
        names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
        description = "New comment for the catalog",
        defaultValue = "")
    String comment;

    @CommandLine.Option(
        names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.RENAME,
        description = "New name for the catalog")
    String newName;
  }

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }
}

/** Name option validator for the catalog command. */
class CatalogNameValidator implements NameValidator {
  /** {@inheritDoc} */
  @Override
  public List<String> getMissingEntities(CliFullName fullName) {
    List<String> missingEntities = Lists.newArrayList();
    if (fullName.getCatalogName() == null) {
      missingEntities.add(CommandEntities.CATALOG);
    }

    return missingEntities;
  }
}
