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
import static org.apache.gravitino.cli.handler.CliHandler.DESCRIPTION_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.HEAD_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.OPTION_LIST_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.PARAMETER_LIST_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.SYNOPSIS_HEADING_STYLE;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.Providers;
import org.apache.gravitino.cli.options.EnableDisableOptions;
import org.apache.gravitino.cli.options.PropertyOptions;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import picocli.CommandLine;

/** Handler for the catalog command. */
@CommandLine.Command(
    name = CommandEntities.CATALOG,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
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

  /** {@inheritDoc} */
  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }
}

/** Handler for the catalog details command. */
@CommandLine.Command(
    name = CommandActions.DETAILS,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Get details of a catalog or get audit information for a catalog")
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
    String catalog = getCatalog();
    GravitinoClient client = buildClient();

    Catalog result = execute(() -> client.loadCatalog(catalog));

    if (audit) {
      printResults(result.auditInfo());
    } else {
      printResults(result);
    }

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }
}

/** Handler for the catalog create command. */
@CommandLine.Command(
    name = CommandActions.CREATE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Create a new catalog")
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
  String provider;

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

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();

    String result =
        execute(
            () -> {
              client.createCatalog(
                  catalog,
                  Providers.catalogType(provider),
                  Providers.internal(provider),
                  comment,
                  properties);

              return catalog + " catalog created";
            });

    printInformation(result);
    return 0;
  }
}

/** Handler for the catalog update command. */
@CommandLine.Command(
    name = CommandActions.DELETE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Delete a catalog")
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
    if (!AreYouSure.really(force)) {
      return 0;
    }

    String catalog = getCatalog();

    GravitinoClient client = buildClient();
    boolean deleted = execute(() -> client.dropCatalog(catalog));

    if (deleted) {
      printInformation(catalog + " deleted.");
    } else {
      printInformation(catalog + " not deleted.");
    }

    return 0;
  }
}

/** Handler for set command. */
@CommandLine.Command(
    name = CommandActions.SET,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Set a property of a catalog")
class CatalogSet extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.ArgGroup(exclusive = false)
  PropertyOptions propertyOptions;

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();

    String result =
        execute(
            () -> {
              CatalogChange change =
                  CatalogChange.setProperty(propertyOptions.property, propertyOptions.value);
              client.alterCatalog(catalog, change);

              return catalog + " property set.";
            });

    printInformation(result);

    return 0;
  }
}

/** Handler for the catalog remove command. */
@CommandLine.Command(
    name = CommandActions.REMOVE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Remove a property from a catalog")
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

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();

    String result =
        execute(
            () -> {
              CatalogChange change = CatalogChange.removeProperty(property);
              client.alterCatalog(catalog, change);

              return property + " property removed.";
            });

    printInformation(result);

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }
}

/** Handler for the catalog properties command. */
@CommandLine.Command(
    name = CommandActions.PROPERTIES,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Display a catalog's properties")
class CatalogProperties extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();

    Map<String, String> result =
        execute(
            () -> {
              Catalog gCatalog = client.loadCatalog(catalog);

              return gCatalog.properties();
            });

    printResults(result);
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return CATALOG_VALIDATOR;
  }
}

/** Handler for the catalog list command. */
@CommandLine.Command(
    name = CommandActions.LIST,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Show all catalogs in a metalake")
class CatalogList extends CliHandler {

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();

    Catalog result = execute(() -> client.loadCatalog(catalog));

    printResults(result);

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the catalog update command. */
@CommandLine.Command(
    name = CommandActions.UPDATE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Update a catalog")
class CatalogUpdate extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.ALL},
      description = "New comment for the catalog",
      defaultValue = "")
  boolean all;

  @CommandLine.ArgGroup(multiplicity = "1")
  UpdateOptions updateOptions;

  /** Update options. */
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

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoAdminClient adminClient = buildAdminClient();
    GravitinoClient client = buildClient();
    String metalake = getMetalake();
    String catalog = getCatalog();

    String result =
        execute(
            () -> {
              if (updateOptions.enableDisableOptions != null) {
                boolean enable = updateOptions.enableDisableOptions.enable;

                if (all && enable) {
                  adminClient.enableMetalake(metalake);
                }

                if (enable) {
                  client.enableCatalog(catalog);

                  return metalake + "." + catalog + " has been enabled.";
                } else {
                  client.disableCatalog(catalog);

                  return metalake + "." + catalog + " has been disabled.";
                }
              }

              if (updateOptions.comment != null) {
                CatalogChange change = CatalogChange.updateComment(updateOptions.comment);
                client.alterCatalog(catalog, change);

                return catalog + " comment changed.";
              }

              CatalogChange change = CatalogChange.rename(name);
              client.alterCatalog(catalog, change);

              return catalog + " name changed.";
            });

    printInformation(result);

    return 0;
  }

  /** {@inheritDoc} */
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
