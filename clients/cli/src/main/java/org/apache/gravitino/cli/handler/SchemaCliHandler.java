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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.options.PropertyOptions;
import org.apache.gravitino.client.GravitinoClient;
import picocli.CommandLine;

/** Handler for schema command. */
@CommandLine.Command(
    name = CommandEntities.SCHEMA,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    description = "Operations on schemas",
    subcommands = {
      SchemaDetails.class,
      SchemaCreate.class,
      SchemaDelete.class,
      SchemaSet.class,
      SchemaRemove.class,
      SchemaProperties.class,
      SchemaList.class
    })
public class SchemaCliHandler implements Runnable {
  /** Validator for schema command name options (excluding the list command). */
  public static final NameValidator SCHEMA_VALIDATOR = new SchemaNameValidator();

  /** Validator for schema list command name options. */
  public static final NameValidator SCHEMA_LIST_VALIDATOR = new SchemaListNameValidator();

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

/** Handler for details command. */
@CommandLine.Command(
    name = CommandActions.DETAILS,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Get details of a schema or get audit information for a schema")
class SchemaDetails extends CliHandler {
  /** The name options, use -n/--name */
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /**
   * Weather to show audit information for the model, use --audit to enable or --no-audit to
   * disable, default is false.
   */
  @CommandLine.Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.AUDIT,
      description = "Show audit information for the schema",
      negatable = true,
      defaultValue = "false")
  boolean audit;

  /**
   * {@inheritDoc}
   *
   * @return The return code.
   * @throws Exception The Exception.
   */
  @Override
  protected Integer doCall() throws Exception {
    Schema result;
    String catalog = getCatalog();
    String schema = getSchema();
    GravitinoClient client = buildClient();

    result = execute(() -> client.loadCatalog(catalog).asSchemas().loadSchema(schema));

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
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema create command. */
@CommandLine.Command(
    name = CommandActions.CREATE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Create a new schema")
class SchemaCreate extends CliHandler {
  /** The name options, use -n/--name */
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** Comment for the Schema, use -c/--comment */
  @CommandLine.Option(
      names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
      description = "Comment for the Schema",
      defaultValue = "")
  String comment;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();
    String schema = getSchema();

    String result =
        execute(
            () -> {
              client.loadCatalog(catalog).asSchemas().createSchema(schema, comment, null);

              return schema + " " + "created";
            });

    printInformation(result);

    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema delete command. */
@CommandLine.Command(
    name = CommandActions.DELETE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Delete a schema")
class SchemaDelete extends CliHandler {
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
      description = "Whether force to delete the schema",
      defaultValue = "false")
  boolean force;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    if (!AreYouSure.really(force)) {
      return 0;
    }

    boolean deleted;
    String catalog = getCatalog();
    String schema = getSchema();

    GravitinoClient client = buildClient();

    deleted = execute(() -> client.loadCatalog(catalog).asSchemas().dropSchema(schema, false));

    if (deleted) {
      printInformation(schema + " deleted.");
    } else {
      printInformation(schema + " not deleted.");
    }

    return 0;
  }

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema set command. */
@CommandLine.Command(
    name = CommandActions.SET,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Set properties of a schema")
class SchemaSet extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** The property related options */
  @CommandLine.ArgGroup(exclusive = false)
  PropertyOptions propertyOptions;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();
    String schema = getSchema();

    String result =
        execute(
            () -> {
              SchemaChange change =
                  SchemaChange.setProperty(propertyOptions.property, propertyOptions.value);
              client.loadCatalog(catalog).asSchemas().alterSchema(schema, change);

              return schema + " property set.";
            });

    printInformation(result);
    return 0;
  }

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema remove command. */
@CommandLine.Command(
    name = CommandActions.REMOVE,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "Remove a property from a schema")
class SchemaRemove extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY},
      required = true,
      description = "The property to remove from the schema")
  String property;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();
    String schema = getSchema();

    String result =
        execute(
            () -> {
              SchemaChange change = SchemaChange.removeProperty(property);
              client.loadCatalog(catalog).asSchemas().alterSchema(schema, change);

              return property + " property removed.";
            });

    printInformation(result);
    return 0;
  }

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema properties command. */
@CommandLine.Command(
    name = CommandActions.PROPERTIES,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "List the properties of a schema")
class SchemaProperties extends CliHandler {

  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();
    String schema = getSchema();

    Map<String, String> result =
        execute(
            () -> {
              Schema gSchema = client.loadCatalog(catalog).asSchemas().loadSchema(schema);
              return gSchema.properties();
            });

    printResults(result);

    return 0;
  }

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema list command. */
@CommandLine.Command(
    name = CommandActions.LIST,
    sortOptions = false,
    headerHeading = HEAD_HEADING_STYLE,
    synopsisHeading = SYNOPSIS_HEADING_STYLE,
    descriptionHeading = DESCRIPTION_HEADING_STYLE,
    parameterListHeading = PARAMETER_LIST_HEADING_STYLE,
    optionListHeading = OPTION_LIST_HEADING_STYLE,
    header = "List the schemas in a catalog")
class SchemaList extends CliHandler {
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    GravitinoClient client = buildClient();
    String catalog = getCatalog();

    String[] schemas =
        execute(
            () -> {
              return client.loadCatalog(catalog).asSchemas().listSchemas();
            });

    if (schemas.length == 0) {
      printInformation("No schemas exist.");
      return 0;
    }

    Schema[] schemaObjects = new Schema[schemas.length];
    for (int i = 0; i < schemas.length; i++) {
      String schemaName = schemas[i];
      Schema gSchema =
          new Schema() {
            @Override
            public String name() {
              return schemaName;
            }

            @Override
            public Audit auditInfo() {
              return null;
            }
          };
      schemaObjects[i] = gSchema;
    }

    printResults(schemaObjects);

    return 0;
  }

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_LIST_VALIDATOR;
  }
}

class SchemaNameValidator extends SchemaListNameValidator {
  @Override
  public List<String> getMissingEntities(CliFullName fullName) {
    List<String> missingEntities = super.getMissingEntities(fullName);
    if (fullName.getSchemaName() == null) {
      missingEntities.add(CommandEntities.SCHEMA);
    }

    return missingEntities;
  }
}

class SchemaListNameValidator implements NameValidator {

  @Override
  public List<String> getMissingEntities(CliFullName fullName) {
    List<String> missingEntities = Lists.newArrayList();
    if (fullName.getCatalogName() == null) {
      missingEntities.add(CommandEntities.CATALOG);
    }

    return missingEntities;
  }
}
