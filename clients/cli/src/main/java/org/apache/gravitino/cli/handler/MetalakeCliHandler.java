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

import static org.apache.gravitino.cli.handler.MetalakeCliHandler.NO_OP_VALIDATOR;

import java.util.ArrayList;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.options.CommonOptions;
import org.apache.gravitino.cli.options.EnableDisableOptions;
import org.apache.gravitino.cli.options.PropertyOptions;
import picocli.CommandLine;

/** Handler for the metalake command. */
@CommandLine.Command(
    name = CommandEntities.METALAKE,
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

  @Override
  public void run() {
    System.out.println("No operation specified, use --help to see available options");
  }
}

/** Handler for the metalake details command. */
@CommandLine.Command(name = CommandActions.DETAILS, description = "Show details of a metalake")
class MetalakeDetails extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

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

  @Override
  protected Integer doCall() throws Exception {
    // TODO implement details command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake create command. */
@CommandLine.Command(name = CommandActions.CREATE, description = "Create a new metalake")
class MetalakeCreate extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

  /** Comment for the Schema, use -c/--comment */
  @CommandLine.Option(
      names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
      description = "Comment for the metalake",
      defaultValue = "")
  String comment;

  @Override
  protected Integer doCall() throws Exception {
    // TODO implement create command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake delete command. */
@CommandLine.Command(name = CommandActions.DELETE, description = "Delete a metalake")
class MetalakeDelete extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

  /**
   * Whether force to delete the catalog, use -f or --force to force delete the catalog, default is
   * false.
   */
  @CommandLine.Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.FORCE,
      description = "Whether force to delete the metalake",
      defaultValue = "false")
  boolean force;

  @Override
  protected Integer doCall() throws Exception {
    // TODO implement delete command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake set command. */
@CommandLine.Command(name = CommandActions.SET, description = "Set a property of a metalake")
class MetalakeSet extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

  @CommandLine.ArgGroup(exclusive = false, heading = "Property options%n")
  PropertyOptions propertyOptions;

  @Override
  protected Integer doCall() throws Exception {
    // TODO implement set command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake remove command. */
@CommandLine.Command(
    name = CommandActions.REMOVE,
    description = "Remove a property from a metalake")
class MetalakeRemove extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY},
      required = true,
      description = "The property to remove from the metalake")
  String property;

  @Override
  protected Integer doCall() throws Exception {
    // TODO implement remove command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake properties command. */
@CommandLine.Command(
    name = CommandActions.PROPERTIES,
    description = "Show properties of a metalake")
class MetalakeProperties extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake update command. */
@CommandLine.Command(name = CommandActions.UPDATE, description = "Update a metalake")
class MetalakeUpdate extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

  @CommandLine.ArgGroup(multiplicity = "1", heading = "update options%n")
  CatalogUpdate.UpdateOptions updateOptions;

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
    // TODO Implement update command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}

/** Handler for the metalake list command. */
@CommandLine.Command(name = CommandActions.LIST, description = "List all metalakes")
class MetalakeList extends CliHandler {
  @CommandLine.Mixin protected CommonOptions commonOptions;

  @Override
  protected Integer doCall() throws Exception {
    // TODO Implement list command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return NO_OP_VALIDATOR;
  }
}
