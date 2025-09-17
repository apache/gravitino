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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import picocli.CommandLine;

/** Handler for schema command. */
@CommandLine.Command(
    name = CommandEntities.SCHEMA,
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

  @CommandLine.Option(
      names = {"-h", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP},
      usageHelp = true,
      description = "display help message")
  boolean usageHelpRequested;

  @Override
  public void run() {
    System.out.println("Schema command");
  }
}

@CommandLine.Command(
    name = CommandActions.DETAILS,
    description = "Get details of a schema or get audit information for a schema")
class SchemaDetails extends CliHandler {
  @CommandLine.Mixin protected SchemaCommonOptions commonOptions;

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

  @Override
  protected Integer doCall() throws Exception {
    // TODO: implement schema details command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema create command. */
@CommandLine.Command(name = CommandActions.CREATE, description = "Create a new schema")
class SchemaCreate extends CliHandler {
  @CommandLine.Mixin protected SchemaCommonOptions commonOptions;

  /** Comment for the Schema, use -c/--comment */
  @CommandLine.Option(
      names = {"-c", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.COMMENT},
      description = "Comment for the Schema",
      defaultValue = "")
  String comment;

  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    // TODO: implement schema create command
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema delete command. */
@CommandLine.Command(name = CommandActions.DELETE, description = "Delete a schema")
class SchemaDelete extends CliHandler {
  @CommandLine.Mixin protected SchemaCommonOptions commonOptions;

  /**
   * Whether force to delete the catalog, use -f or --force to force delete the catalog, default is
   * false.
   */
  @CommandLine.Option(
      names = GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.FORCE,
      description = "Whether force to delete the schema",
      defaultValue = "false")
  boolean force;

  @Override
  protected Integer doCall() throws Exception {
    // TODO: implement schema delete command
    return 0;
  }

  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema set command. */
@CommandLine.Command(name = CommandActions.SET, description = "Set properties of a schema")
class SchemaSet extends CliHandler {

  @CommandLine.Mixin protected SchemaCommonOptions commonOptions;

  @CommandLine.ArgGroup(exclusive = false)
  PropertyOptions propertyOptions;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    // TODO implement command
    return 0;
  }

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema remove command. */
@CommandLine.Command(name = CommandActions.REMOVE, description = "Remove a property from a schema")
class SchemaRemove extends CliHandler {
  @CommandLine.Mixin protected SchemaCommonOptions commonOptions;

  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY},
      required = true,
      description = "The property to remove from the schema")
  String property;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    // TODO implement command
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
    description = "List the properties of a schema")
class SchemaProperties extends CliHandler {

  @CommandLine.Mixin protected SchemaCommonOptions commonOptions;
  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    // TODO implement command
    return 0;
  }

  /** {inheritDoc} */
  @Override
  protected NameValidator createValidator() {
    return SchemaCliHandler.SCHEMA_VALIDATOR;
  }
}

/** Handler for schema list command. */
@CommandLine.Command(name = CommandActions.LIST, description = "List the schemas in a catalog")
class SchemaList extends CliHandler {
  @CommandLine.Mixin protected SchemaCommonOptions commonOptions;

  /** {inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    // TODO implement command
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

class SchemaCommonOptions extends CommonOptions {
  /** name of the entity, use --name/-n to specify the name */
  @CommandLine.Option(
      names = {"-n", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME},
      required = true,
      description = "name of the entity")
  String name;
}
