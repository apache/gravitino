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

import static org.apache.gravitino.cli.handler.ModelCliHandler.MODEL_LIST_VALIDATOR;
import static org.apache.gravitino.cli.handler.ModelCliHandler.MODEL_NAME_VALIDATOR;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.options.CommonOptions;
import org.apache.gravitino.cli.options.PropertyOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Handler for model commands. */
@Command(
    name = CommandEntities.MODEL,
    description = "Operations on model",
    subcommands = {
      ModelCreate.class,
      ModelDetails.class,
      ModelList.class,
      ModelDelete.class,
      ModelSet.class,
      ModelRemove.class,
      ModelUpdate.class
    })
public class ModelCliHandler implements Runnable {
  /** Validator for name options of model commands. except list command. */
  public static final NameValidator MODEL_NAME_VALIDATOR = new ModelNameValidator();
  /** Validator for name options of model list command. */
  public static final NameValidator MODEL_LIST_VALIDATOR = new ModelListValidator();
  /** Joiner for comma separated values. */
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

  /** display help message, use --help/-h to display a help message */
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
            CommandActions.SET,
            CommandActions.DELETE,
            CommandActions.REMOVE
          });

  /** Runs the command. */
  @Override
  public void run() {
    System.out.println(CommandEntities.MODEL + " " + "[command] [options]");
    System.out.println("Available model commands: " + VALID_ACTIONS);
  }
}

/** Handler for a model create command. */
@Command(name = CommandActions.CREATE, description = "Create a new model")
class ModelCreate extends CliHandler {
  @CommandLine.Mixin protected ModelCommonOptions commonOptions;

  @Option(
      names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
      description = "Comment for the model",
      defaultValue = "")
  String comment;

  @Option(
      names = {"-p", "--properties"},
      description = "Properties in key=value format",
      split = ",")
  Map<String, String> properties;

  @Override
  public Integer call() throws Exception {
    if (properties == null) {
      properties = new HashMap<>();
    }

    return super.call();
  }

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model details command. */
@Command(name = CommandActions.DETAILS, description = "Get details of a model")
class ModelDetails extends CliHandler {
  @CommandLine.Mixin protected ModelCommonOptions commonOptions;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.AUDIT,
      description = "Show audit information for the model",
      negatable = true,
      defaultValue = "false")
  boolean audit;

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model list command. */
@Command(
    name = CommandActions.LIST,
    description = "List models in specified metalake.catalog.schema")
class ModelList extends CliHandler {

  @CommandLine.Mixin protected ModelCommonOptions common;

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return MODEL_LIST_VALIDATOR;
  }
}

/** Handler for model delete command. */
@Command(
    name = CommandActions.DELETE,
    description = "Delete model in specified metalake.catalog.schema")
class ModelDelete extends CliHandler {

  @CommandLine.Mixin protected ModelCommonOptions common;

  /** Whether to force delete the model, use -f or --force to force delete the model */
  @CommandLine.Option(
      names = "--force",
      description = "Whether to force delete the model",
      defaultValue = "false")
  boolean force;

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model set command. */
@Command(
    name = CommandActions.SET,
    usageHelpAutoWidth = true,
    description = "Set property of a model version by alias or version")
class ModelSet extends CliHandler {
  @CommandLine.Mixin protected ModelCommonOptions commonOptions;
  @CommandLine.ArgGroup() VersionOrAlias versionOrAlias;

  @CommandLine.ArgGroup(exclusive = false)
  PropertyOptions propertyOptions;

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model remove command. */
@Command(
    name = CommandActions.REMOVE,
    usageHelpAutoWidth = true,
    description = "Remove property of a model version by alias or version")
class ModelRemove extends CliHandler {
  @CommandLine.Mixin ModelCommonOptions common;

  @CommandLine.ArgGroup() VersionOrAlias versionOrAlias;

  @CommandLine.ArgGroup(multiplicity = "1", heading = "Remove options%n")
  RemoveOptions removeOptions;

  static class RemoveOptions {
    @Option(
        names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY,
        required = true,
        description = "Property name to remove")
    String removedProperty;

    @Option(
        names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.REMOVE_ALIAS,
        required = true,
        description = "Alias to remove (only " + "for model-version)")
    String removedAlias;
  }

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model update command. */
@Command(
    name = CommandActions.UPDATE,
    usageHelpAutoWidth = true,
    description = "Update model or model version")
class ModelUpdate extends CliHandler {
  @CommandLine.Mixin ModelCommonOptions common;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.ALIAS,
      description = "Alias of the model " + "version")
  String[] alias;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.VERSION,
      description = "Version of the model version")
  Integer version;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT,
      description = "Comment for the model or model version")
  String comment;

  @CommandLine.ArgGroup(heading = "Update target%n")
  UpdateOptions updateOptions;

  static class UpdateOptions {
    @Option(
        names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.URIS},
        description = "URIs for link model",
        split = ",")
    Map<String, String> uris;

    @Option(
        names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.RENAME,
        description = "New name for the model")
    String newName;

    @Option(
        names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NEW_URI,
        description = "New URI for the model " + "version")
    String newUri;

    @Option(
        names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NEW_ALIAS,
        split = ",",
        description = "New aliases for the model version")
    List<String> newAliases;
  }

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected void validateOptions(NameValidator validator) {
    super.validateOptions(validator);
    if (alias != null && version != null && (updateOptions != null && updateOptions.uris != null)) {
      throw new RuntimeException(
          "Cant not specify both alias and version when update a model version");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Name option validator for the model command. */
class ModelNameValidator extends ModelListValidator {
  /** {@inheritDoc} */
  @Override
  public List<String> getMissingEntities(CliFullName fullName) {
    List<String> missingEntities = super.getMissingEntities(fullName);

    if (fullName.getModelName() == null) {
      missingEntities.add(CommandEntities.MODEL);
    }

    return missingEntities;
  }
}

/** Name option validator for the model list command. */
class ModelListValidator implements NameValidator {
  /** {@inheritDoc} */
  @Override
  public List<String> getMissingEntities(CliFullName fullName) {
    List<String> missingEntities = Lists.newArrayList();
    if (fullName.getCatalogName() == null) {
      missingEntities.add(CommandEntities.CATALOG);
    }
    if (fullName.getSchemaName() == null) {
      missingEntities.add(CommandEntities.SCHEMA);
    }

    return missingEntities;
  }
}

/** Common options for the model version command. */
class VersionOrAlias {
  @CommandLine.Option(
      names = "--alias",
      required = true,
      description = "Alias of the model version")
  String alias;

  @CommandLine.Option(
      names = "--version",
      required = true,
      description = "Version of the model version")
  int version;
}

class ModelCommonOptions extends CommonOptions {
  /** name of the entity, use --name/-n to specify the name */
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;
}
