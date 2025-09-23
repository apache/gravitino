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

import static org.apache.gravitino.cli.handler.CliHandler.DESCRIPTION_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.HEAD_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.OPTION_LIST_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.PARAMETER_LIST_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.CliHandler.SYNOPSIS_HEADING_STYLE;
import static org.apache.gravitino.cli.handler.MetalakeCliHandler.NO_OP_VALIDATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.options.EnableDisableOptions;
import org.apache.gravitino.cli.options.PropertyOptions;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoMetalake;
import picocli.CommandLine;

/** Handler for the metalake command. */
@CommandLine.Command(
    name = CommandEntities.METALAKE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    description = "Operations on metalake",
    subcommands = {
      MetalakeDetails.class,
      MetalakeCreate.class,
      MetalakeDelete.class,
      MetalakeSet.class,
      MetalakeRemove.class,
      MetalakeProperties.class,
      MetalakeUpdate.class,
      MetalakeList.class
    })
public class MetalakeCliHandler implements Runnable {
  /** Validator for name options, which does not validate */
  public static final NameValidator NO_OP_VALIDATOR = fullName -> new ArrayList<>();

  /** display help message, use --help/-h to display a help message */
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

/** Handler for the metalake details command. */
@CommandLine.Command(
    name = CommandActions.DETAILS,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header =
        "Show details of a metalake or get audit information of the metalake(use --audit option)")
class MetalakeDetails extends CliHandler {

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
    String metalake = getMetalake();
    GravitinoMetalake gMetalake;

    try (GravitinoClient client = buildClient()) {
      gMetalake = execute(() -> client.loadMetalake(metalake));
    }

    if (audit) {
      printResults(gMetalake.auditInfo());
    } else {
      printResults(gMetalake);
    }

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake create command. */
@CommandLine.Command(
    name = CommandActions.CREATE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Create a new metalake")
class MetalakeCreate extends CliHandler {

  /** Comment for the Schema, use -c/--comment */
  @CommandLine.Option(
      names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
      description = "Comment for the metalake",
      defaultValue = "")
  String comment;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    String metalake = getMetalake();

    try (GravitinoAdminClient client = buildAdminClient()) {
      execute(() -> client.createMetalake(metalake, comment, null));
    }

    printInformation(metalake + " created");
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake delete command. */
@CommandLine.Command(
    name = CommandActions.DELETE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Delete a metalake")
class MetalakeDelete extends CliHandler {

  /**
   * Whether force to delete the catalog, use -f or --force to force delete the catalog, default is
   * false.
   */
  @CommandLine.Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.FORCE,
      description = "Whether force to delete the metalake",
      defaultValue = "false")
  boolean force;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    if (!AreYouSure.really(force)) {
      return 0;
    }
    boolean deleted;

    String metalake = getMetalake();

    try (GravitinoAdminClient client = buildAdminClient()) {
      deleted = execute(() -> client.dropMetalake(metalake));
    }

    if (deleted) {
      printInformation(metalake + " deleted.");
    } else {
      printInformation(metalake + " not deleted.");
    }

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake set command. */
@CommandLine.Command(
    name = CommandActions.SET,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Set a property of a metalake")
class MetalakeSet extends CliHandler {

  @CommandLine.ArgGroup(exclusive = false, heading = "Property options%n")
  PropertyOptions propertyOptions;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    String metalake = getMetalake();

    try (GravitinoAdminClient client = buildAdminClient()) {
      execute(
          () -> {
            MetalakeChange change =
                MetalakeChange.setProperty(propertyOptions.property, propertyOptions.value);
            client.alterMetalake(metalake, change);
          });
    }
    printInformation(metalake + " property set.");
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake remove command. */
@CommandLine.Command(
    name = CommandActions.REMOVE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Remove a property from a metalake")
class MetalakeRemove extends CliHandler {

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY},
      required = true,
      description = "The property to remove from the metalake")
  String property;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    String metalake = getMetalake();

    try (GravitinoAdminClient client = buildAdminClient()) {
      execute(
          () -> {
            MetalakeChange change = MetalakeChange.removeProperty(property);
            client.alterMetalake(metalake, change);
          });
    }

    printInformation(property + " property removed.");
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake properties command. */
@CommandLine.Command(
    name = CommandActions.PROPERTIES,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Display the properties of a metalake")
class MetalakeProperties extends CliHandler {

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    String metalake = getMetalake();
    Map<String, String> properties;

    try (GravitinoAdminClient client = buildAdminClient(); ) {
      properties = execute(() -> client.loadMetalake(metalake).properties());
    }

    printResults(properties);
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake update command. */
@CommandLine.Command(
    name = CommandActions.UPDATE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Update a metalake")
class MetalakeUpdate extends CliHandler {

  @CommandLine.ArgGroup(multiplicity = "1", heading = "update options%n")
  CatalogUpdate.UpdateOptions updateOptions;

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.ALL},
      description = "Whether enable all catalog when enable the matalake",
      defaultValue = "")
  boolean all;

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
    String metalake = getMetalake();

    try (GravitinoAdminClient adminClient = buildAdminClient();
        GravitinoClient client = buildClient()) {
      return execute(
          () -> {
            if (updateOptions.enableDisableOptions != null) {
              boolean enabled = updateOptions.enableDisableOptions.enable;
              if (all && enabled) {
                adminClient.enableMetalake(metalake);
                String[] catalogs = client.listCatalogs();
                Arrays.stream(catalogs).forEach(client::enableCatalog);

                printInformation(
                    metalake
                        + " has been enabled, and all catalogs in this metalake have been enabled.");
                return 0;
              }

              if (enabled) {
                adminClient.enableMetalake(metalake);

                printInformation(metalake + " has been enabled.");
              } else {
                adminClient.disableMetalake(metalake);

                printInformation(metalake + " has been disabled.");
              }

              return 0;
            }

            if (updateOptions.comment != null && !updateOptions.comment.isEmpty()) {
              MetalakeChange change = MetalakeChange.updateComment(updateOptions.comment);
              adminClient.alterMetalake(metalake, change);

              printInformation(metalake + " comment changed.");
              return 0;
            }

            MetalakeChange change = MetalakeChange.rename(updateOptions.newName);
            adminClient.alterMetalake(metalake, change);

            printInformation(metalake + " name changed.");
            return 0;
          });
    }
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake list command. */
@CommandLine.Command(
    name = CommandActions.LIST,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "List all metalakes")
class MetalakeList extends CliHandler {

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoMetalake[] gMetalakes;
    try (GravitinoAdminClient adminClient = buildAdminClient()) {
      gMetalakes = execute(() -> execute(adminClient::listMetalakes));
    }

    if (gMetalakes.length == 0) {
      printInformation("No metalakes exist.");
      return 0;
    }

    printResults(gMetalakes);
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}
