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
import static org.apache.gravitino.cli.handler.ModelCliHandler.MODEL_LIST_VALIDATOR;
import static org.apache.gravitino.cli.handler.ModelCliHandler.MODEL_NAME_VALIDATOR;
import static org.apache.gravitino.cli.handler.ModelCliHandler.getOneAlias;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.MainCli;
import org.apache.gravitino.cli.options.PropertyOptions;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersionChange;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Handler for model commands. */
@Command(
    name = CommandEntities.MODEL,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
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

  /**
   * Get one alias from the aliases option
   *
   * @param aliases The value of aliases option
   * @return The first alias.
   */
  public static String getOneAlias(String[] aliases) {
    if (aliases == null || aliases.length > 1) {
      System.err.println(ErrorMessages.MULTIPLE_ALIASES_COMMAND_ERROR);
      MainCli.exit(-1);
    }

    return aliases[0];
  }

  /** Runs the command. */
  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }
}

/** Handler for a model create command. */
@Command(
    name = CommandActions.CREATE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Create a model")
class ModelCreate extends CliHandler {
  /** Name of the entity, use -n/--name specify the value */
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** Comment of a model, use -c/--comment specify the value */
  @Option(
      names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
      description = "Comment for the model",
      defaultValue = "")
  String comment;

  /** Property of a model, use -p/--property specify the value */
  @Option(
      names = {"-p", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTIES},
      description = "Properties in key=value format",
      split = ",")
  Map<String, String> properties;

  /** {@inheritDoc} */
  @Override
  public Integer call() throws Exception {
    if (properties == null) {
      properties = new HashMap<>();
    }

    return super.call();
  }

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();
    String schema = getSchema();
    String model = getModel();

    NameIdentifier name = NameIdentifier.of(schema, model);

    Model registeredModel =
        execute(
            () -> {
              ModelCatalog modelCatalog = client.loadCatalog(catalog).asModelCatalog();
              return modelCatalog.registerModel(name, comment, properties);
            });

    if (registeredModel != null) {
      printInformation("Successful register " + registeredModel.name() + ".");
    } else {
      exitWithError(ErrorMessages.REGISTER_FAILED + model + ".");
    }

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model details command. */
@Command(
    name = CommandActions.DETAILS,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Get details of a model or get audit information for a model")
class ModelDetails extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.AUDIT,
      description = "Show audit information for the model",
      negatable = true,
      defaultValue = "false")
  boolean audit;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    String catalog = getCatalog();
    String schema = getSchema();
    String model = getModel();
    NameIdentifier name = NameIdentifier.of(schema, model);
    GravitinoClient client = buildClient();

    Model gModel =
        execute(
            () -> {
              ModelCatalog modelCatalog = client.loadCatalog(catalog).asModelCatalog();
              return modelCatalog.getModel(name);
            });

    if (gModel == null) {
      return 1;
    }

    if (audit) {
      printResults(gModel.auditInfo());
    } else {
      printResults(gModel);
    }

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model list command. */
@Command(
    name = CommandActions.LIST,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "List model in a catalog.schema")
class ModelList extends CliHandler {

  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    NameIdentifier[] models;
    String catalog = getCatalog();
    String schema = getSchema();
    Namespace name = Namespace.of(schema);

    try (GravitinoClient client = buildClient()) {
      models = execute(() -> client.loadCatalog(catalog).asModelCatalog().listModels(name));
    }

    if (models.length == 0) {
      printInformation("No models exist.");
    } else {
      Model[] modelsArr = Arrays.stream(models).map(this::getModel).toArray(Model[]::new);
      printResults(modelsArr);
    }

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_LIST_VALIDATOR;
  }

  private Model getModel(NameIdentifier modelIdent) {
    return new Model() {
      @Override
      public String name() {
        return modelIdent.name();
      }

      @Override
      public int latestVersion() {
        return 0;
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }
}

/** Handler for model delete command. */
@Command(
    name = CommandActions.DELETE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Delete a model")
class ModelDelete extends CliHandler {

  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** Whether to force delete the model, use -f or --force to force delete the model */
  @CommandLine.Option(
      names = "--force",
      description = "Whether to force delete the model",
      defaultValue = "false")
  boolean force;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    if (!AreYouSure.really(force)) {
      return 0;
    }
    String catalog = getCatalog();
    String schema = getSchema();
    String model = getModel();
    boolean deleted;

    try (GravitinoClient client = buildClient()) {
      deleted =
          execute(
              () ->
                  client
                      .loadCatalog(catalog)
                      .asModelCatalog()
                      .deleteModel(NameIdentifier.of(schema, model)));
    }

    if (deleted) {
      printInformation(model + " deleted.");
    } else {
      printInformation(model + " not deleted.");
    }

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }
}

/** Handler for model set command. */
@Command(
    name = CommandActions.SET,
    usageHelpAutoWidth = true,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Set property of a model version by alias or version")
class ModelSet extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.ArgGroup() VersionOrAlias versionOrAlias;

  @CommandLine.ArgGroup(exclusive = false)
  PropertyOptions propertyOptions;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    String catalog = getCatalog();
    String schema = getSchema();
    String model = getModel();
    int result;
    NameIdentifier name = NameIdentifier.of(schema, model);

    try (GravitinoClient client = buildClient()) {
      result =
          execute(
              () -> {
                if (versionOrAlias != null) {
                  return setModelVersionProperty(client, catalog, name);
                }

                return setModelProperty(client, catalog, name);
              });
    }

    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }

  private int setModelProperty(GravitinoClient client, String catalog, NameIdentifier ident) {
    ModelChange change = ModelChange.setProperty(propertyOptions.property, propertyOptions.value);
    client.loadCatalog(catalog).asModelCatalog().alterModel(ident, change);
    printInformation(ident.name() + " property set.");
    return 0;
  }

  private int setModelVersionProperty(
      GravitinoClient client, String catalog, NameIdentifier ident) {
    ModelVersionChange change =
        ModelVersionChange.setProperty(propertyOptions.property, propertyOptions.value);
    if (versionOrAlias.alias == null) {
      client
          .loadCatalog(catalog)
          .asModelCatalog()
          .alterModelVersion(ident, versionOrAlias.version, change);
      printInformation(ident.name() + " version " + versionOrAlias.version + " property set.");

      return 0;
    }

    client
        .loadCatalog(catalog)
        .asModelCatalog()
        .alterModelVersion(ident, versionOrAlias.alias, change);
    printInformation(ident.name() + " alias " + versionOrAlias.alias + " property set.");

    return 0;
  }
}

/** Handler for model remove command. */
@Command(
    name = CommandActions.REMOVE,
    usageHelpAutoWidth = true,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header =
        "Remove property/alias of a model version by alias or version, or remove property of a model")
class ModelRemove extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

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
    String[] removedAlias;
  }

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    String catalog = getCatalog();
    String schema = getSchema();
    String model = getModel();
    int result;

    NameIdentifier ident = NameIdentifier.of(schema, model);

    try (GravitinoClient client = buildClient()) {
      result =
          execute(
              () -> {
                if (versionOrAlias == null) {
                  return removeModelProperty(client, catalog, ident);
                }

                if (removeOptions.removedProperty != null) {
                  return removeModelVersionProperty(client, catalog, ident);
                }

                return removeModelVersionAlias(client, catalog, ident);
              });
    }

    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }

  private int removeModelProperty(GravitinoClient client, String catalog, NameIdentifier ident) {
    ModelChange change = ModelChange.removeProperty(removeOptions.removedProperty);
    client.loadCatalog(catalog).asModelCatalog().alterModel(ident, change);
    printInformation(removeOptions.removedProperty + " property removed.");

    return 0;
  }

  private int removeModelVersionProperty(
      GravitinoClient client, String catalog, NameIdentifier ident) {
    ModelVersionChange change = ModelVersionChange.removeProperty(removeOptions.removedProperty);

    if (versionOrAlias.alias == null) {
      client
          .loadCatalog(catalog)
          .asModelCatalog()
          .alterModelVersion(ident, versionOrAlias.version, change);
      printInformation(
          ident.name()
              + " version "
              + versionOrAlias.version
              + "property "
              + removeOptions.removedProperty
              + " "
              + "removed.");

      return 0;
    }

    client
        .loadCatalog(catalog)
        .asModelCatalog()
        .alterModelVersion(ident, versionOrAlias.alias, change);
    printInformation(
        ident.name()
            + " alias "
            + versionOrAlias.alias
            + "property "
            + removeOptions.removedProperty
            + " "
            + "removed.");

    return 0;
  }

  private int removeModelVersionAlias(
      GravitinoClient client, String catalog, NameIdentifier ident) {
    ModelVersionChange change =
        ModelVersionChange.updateAliases(new String[0], removeOptions.removedAlias);
    if (versionOrAlias.alias == null) {
      client
          .loadCatalog(catalog)
          .asModelCatalog()
          .alterModelVersion(ident, versionOrAlias.version, change);
      printInformation(ident.name() + " version " + versionOrAlias.version + " aliases changed.");

      return 0;
    }

    client
        .loadCatalog(catalog)
        .asModelCatalog()
        .alterModelVersion(ident, versionOrAlias.alias, change);
    printInformation(ident.name() + " version " + versionOrAlias.alias + " aliases changed.");

    return 0;
  }
}

/** Handler for model update command. */
@Command(
    name = CommandActions.UPDATE,
    usageHelpAutoWidth = true,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Update model or model version")
class ModelUpdate extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.ALIAS,
      description = "Alias of the model " + "version")
  String[] aliases;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.VERSION,
      description = "Version of the model version")
  Integer version;

  @Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT,
      description = "Comment for the model or model version")
  String comment;

  /** Property of a model, use -p/--property specify the value */
  @Option(
      names = {"-p", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY},
      description = "Properties in key=value format",
      split = ",")
  Map<String, String> properties;

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
    String[] newAliases;
  }

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    int result;
    String catalog = getCatalog();
    String schema = getSchema();
    String model = getModel();
    NameIdentifier modelIdent = NameIdentifier.of(schema, model);

    try (GravitinoClient client = buildClient()) {
      result =
          execute(
              () -> {
                if (updateOptions.uris != null) {
                  return linkModelVersion(client, catalog, modelIdent);
                }

                if (updateOptions.newName != null) {
                  return updateModelName(client, catalog, modelIdent);
                }

                if (updateOptions.newUri != null) {
                  return updateModelVersionUri(client, catalog, modelIdent);
                }

                if (updateOptions.newAliases != null) {
                  return updateModelVersionAliases(client, catalog, modelIdent);
                }

                return updateComment(client, catalog, modelIdent);
              });
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return MODEL_NAME_VALIDATOR;
  }

  private int updateModelName(GravitinoClient client, String catalog, NameIdentifier modelIdent) {
    ModelChange change = ModelChange.rename(name);
    client.loadCatalog(catalog).asModelCatalog().alterModel(modelIdent, change);

    return 0;
  }

  private int updateModelVersionUri(
      GravitinoClient client, String catalog, NameIdentifier modelIdent) {
    checkVersionOrAlias();
    ModelVersionChange change = ModelVersionChange.updateUri(updateOptions.newUri);

    if (aliases != null) {
      String modelAlias = ModelCliHandler.getOneAlias(aliases);
      client
          .loadCatalog(catalog)
          .asModelCatalog()
          .alterModelVersion(modelIdent, modelAlias, change);
    } else {
      client.loadCatalog(catalog).asModelCatalog().alterModelVersion(modelIdent, version, change);
    }

    return 0;
  }

  private int linkModelVersion(GravitinoClient client, String catalog, NameIdentifier modelIdent) {
    if (version != null) {
      exitWithError("Can't specify the version option when link a model version");
    }

    ModelCatalog modelCatalog = client.loadCatalog(catalog).asModelCatalog();
    modelCatalog.linkModelVersion(modelIdent, updateOptions.newUri, aliases, comment, properties);

    return 0;
  }

  private int updateModelVersionAliases(
      GravitinoClient client, String catalog, NameIdentifier modelIdent) {
    checkVersionOrAlias();
    ModelVersionChange change =
        ModelVersionChange.updateAliases(updateOptions.newAliases, new String[0]);

    if (aliases != null) {
      String alias = ModelCliHandler.getOneAlias(aliases);
      client.loadCatalog(catalog).asModelCatalog().alterModelVersion(modelIdent, alias, change);
    } else {
      client.loadCatalog(catalog).asModelCatalog().alterModelVersion(modelIdent, version, change);
    }

    return 0;
  }

  private int updateComment(GravitinoClient client, String catalog, NameIdentifier modelIdent) {
    if (version == null && aliases == null) {
      return updateModelComment(client, catalog, modelIdent);
    }

    return updateModelVersionComment(client, catalog, modelIdent);
  }

  private int updateModelComment(
      GravitinoClient client, String catalog, NameIdentifier modelIdent) {
    ModelChange change = ModelChange.updateComment(comment);
    client.loadCatalog(catalog).asModelCatalog().alterModel(modelIdent, change);

    return 0;
  }

  private int updateModelVersionComment(
      GravitinoClient client, String catalog, NameIdentifier modelIdent) {
    checkVersionOrAlias();

    ModelVersionChange change = ModelVersionChange.updateComment(comment);
    if (aliases != null) {
      String alias = getOneAlias(aliases);
      client.loadCatalog(catalog).asModelCatalog().alterModelVersion(modelIdent, alias, change);
    } else {
      client.loadCatalog(catalog).asModelCatalog().alterModelVersion(modelIdent, version, change);
    }

    return 0;
  }

  private void checkVersionOrAlias() {
    if (version == null && aliases == null) {
      exitWithError("Must specify either alias or version");
    }

    if (version != null && aliases != null && aliases.length >= 1) {
      exitWithError("Cannot specify both alias and version");
    }
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
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.ALIAS,
      required = true,
      description = "Alias of the model version")
  String alias;

  @CommandLine.Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.VERSION,
      required = true,
      description = "Version of the model version")
  int version;
}
