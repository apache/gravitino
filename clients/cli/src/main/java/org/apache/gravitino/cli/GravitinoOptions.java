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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/** Gravitino Command line options */
public class GravitinoOptions {
  /** Represents the audit option. */
  public static final String AUDIT = "audit";
  /** Represents the auto option. */
  public static final String AUTO = "auto";
  /** Represents the client option. */
  public static final String CLIENT = "client";
  /** Represents the columnfile option. */
  public static final String COLUMNFILE = "columnfile";
  /** Represents the comment option. */
  public static final String COMMENT = "comment";
  /** Represents the datatype option. */
  public static final String DATATYPE = "datatype";
  /** Represents the default option. */
  public static final String DEFAULT = "default";
  /** Represents the distribution option. */
  public static final String DISTRIBUTION = "distribution";
  /** Represents the fileset option. */
  public static final String FILESET = "fileset";
  /** Represents the force option. */
  public static final String FORCE = "force";
  /** Represents the group option. */
  public static final String GROUP = "group";
  /** Represents the help option. */
  public static final String HELP = "help";
  /** Represents the option to ignore an operation. */
  public static final String IGNORE = "ignore";
  /** Represents the index option for a column or property. */
  public static final String INDEX = "index";
  /** Represents the login option. */
  public static final String LOGIN = "login";
  /** Represents the metalake option. */
  public static final String METALAKE = "metalake";
  /** Represents the name of an object. */
  public static final String NAME = "name";
  /** Represents the nullability option. */
  public static final String NULL = "null";
  /** Represents the output option for command results. */
  public static final String OUTPUT = "output";
  /** Represents the owner of an object. */
  public static final String OWNER = "owner";
  /** Represents the partition option. */
  public static final String PARTITION = "partition";
  /** Represents the position of a column. */
  public static final String POSITION = "position";
  /** Represents the privilege option. */
  public static final String PRIVILEGE = "privilege";
  /** Represents the properties option. */
  public static final String PROPERTIES = "properties";
  /** Represents a single property option. */
  public static final String PROPERTY = "property";
  /** Represents the quiet mode option. */
  public static final String QUIET = "quiet";
  /** Represents the provider option for a catalog. */
  public static final String PROVIDER = "provider";
  /** Represents the rename option for an object. */
  public static final String RENAME = "rename";
  /** Represents the role option. */
  public static final String ROLE = "role";
  /** Represents the server option. */
  public static final String SERVER = "server";
  /** Represents the simple output format option. */
  public static final String SIMPLE = "simple";
  /** Represents the sort order option for a column. */
  public static final String SORTORDER = "sortorder";
  /** Represents the tag option for an object. */
  public static final String TAG = "tag";
  /** Represents the URL option for a server. */
  public static final String URL = "url";
  /** Represents the user option for authentication. */
  public static final String USER = "user";
  /** Represents the value of a property. */
  public static final String VALUE = "value";
  /** Represents the version of the application. */
  public static final String VERSION = "version";
  /** Represents the 'all' option. */
  public static final String ALL = "all";
  /** Represents the enable option. */
  public static final String ENABLE = "enable";
  /** Represents the disable option. */
  public static final String DISABLE = "disable";
  /** Represents the alias option for a catalog. */
  public static final String ALIAS = "alias";
  /** Represents the Uris option for a catalog. */
  public static final String URIS = "uris";
  // TODO: temporary option for model version update, it will be refactored in the future, just
  // prove the E2E flow.
  /** Represents the new uri option for a model version. */
  public static final String NEW_URI = "newuri";
  // TODO: temporary option for model version update, it will be refactored in the future, just
  // prove the E2E flow.
  /** Represents the new alias option for a model version. */
  public static final String NEW_ALIAS = "newalias";
  // TODO: temporary option for model version update, it will be refactored in the future, just
  // prove the E2E flow.
  /** Represents the remove alias option for a model version. */
  public static final String REMOVE_ALIAS = "removealias";

  /**
   * Builds and returns the CLI options for Gravitino.
   *
   * @return Valid CLI command options.
   */
  public Options options() {
    Options options = new Options();

    // Add options using helper method to avoid repetition
    options.addOption(createSimpleOption("h", HELP, "command help information"));
    options.addOption(createSimpleOption(null, CLIENT, "Gravitino client version"));
    options.addOption(createSimpleOption("s", SERVER, "Gravitino server version"));
    options.addOption(createArgOption("u", URL, "Gravitino URL (default: http://localhost:8090)"));
    options.addOption(createArgOption("n", NAME, "full entity name (dot separated)"));
    options.addOption(createArgOption("m", METALAKE, "metalake name"));
    options.addOption(createSimpleOption("i", IGNORE, "ignore client/sever version check"));
    options.addOption(createSimpleOption("a", AUDIT, "display audit information"));
    options.addOption(createSimpleOption(null, SIMPLE, "simple authentication"));
    options.addOption(createArgOption(null, LOGIN, "user name"));
    options.addOption(createSimpleOption("x", INDEX, "display index information"));
    options.addOption(createSimpleOption("d", DISTRIBUTION, "display distribution information"));
    options.addOption(createSimpleOption(PARTITION, "display partition information"));
    options.addOption(createSimpleOption("o", OWNER, "display entity owner"));
    options.addOption(createSimpleOption(null, SORTORDER, "display sortorder information"));
    options.addOption(createSimpleOption(null, ENABLE, "enable entities"));
    options.addOption(createSimpleOption(null, DISABLE, "disable entities"));
    options.addOption(createSimpleOption(null, QUIET, "quiet mode"));

    // Create/update options
    options.addOption(createArgOption(RENAME, "new entity name"));
    options.addOption(createArgOption("c", COMMENT, "entity comment"));
    options.addOption(createArgOption("P", PROPERTY, "property name"));
    options.addOption(createArgOption("V", VALUE, "property value"));
    options.addOption(
        createArgOption(
            "z", PROVIDER, "provider one of hadoop, hive, mysql, postgres, iceberg, kafka"));
    options.addOption(createArgOption("l", USER, "user name"));
    options.addOption(createArgOption("g", GROUP, "group name"));
    options.addOption(createArgOption(DATATYPE, "column data type"));
    options.addOption(createArgOption(POSITION, "position of column"));
    options.addOption(createArgOption(NULL, "column value can be null (true/false)"));
    options.addOption(createArgOption(AUTO, "column value auto-increments (true/false)"));
    options.addOption(createArgOption(DEFAULT, "default column value"));
    options.addOption(createArgOption(COLUMNFILE, "CSV file describing columns"));
    options.addOption(createSimpleOption(null, ALL, "on all entities"));

    // model options
    options.addOption(createArgOption(null, URIS, "model version URIs"));
    options.addOption(createArgsOption(null, ALIAS, "model aliases"));
    options.addOption(createArgOption(null, VERSION, "Gravitino client version"));
    options.addOption(createArgOption(null, NEW_URI, "New uri of a model version"));
    options.addOption(createArgsOption(null, NEW_ALIAS, "New alias of a model version"));
    options.addOption(createArgsOption(null, REMOVE_ALIAS, "Remove alias of a model version"));

    // Options that support multiple values
    options.addOption(createArgsOption("p", PROPERTIES, "property name/value pairs"));
    options.addOption(createArgsOption("t", TAG, "tag name"));
    options.addOption(createArgsOption(null, PRIVILEGE, "privilege(s)"));
    options.addOption(createArgsOption("r", ROLE, "role name"));

    // Force delete entities and rename metalake operations
    options.addOption(createSimpleOption("f", FORCE, "force operation"));

    options.addOption(createArgOption(OUTPUT, "output format (plain/table)"));

    return options;
  }

  /**
   * Helper method to create an Option that does not require arguments.
   *
   * @param shortName The option name as a single letter
   * @param longName The long option name.
   * @param description The option description.
   * @return The Option object.
   */
  public Option createSimpleOption(String shortName, String longName, String description) {
    return new Option(shortName, longName, false, description);
  }

  /**
   * Helper method to create an Option that does not require arguments.
   *
   * @param longName The long option name.
   * @param description The option description.
   * @return The Option object.
   */
  public Option createSimpleOption(String longName, String description) {
    return new Option(null, longName, false, description);
  }

  /**
   * Helper method to create an Option that requires an argument.
   *
   * @param shortName The option name as a single letter
   * @param longName The long option name.
   * @param description The option description.
   * @return The Option object.
   */
  public Option createArgOption(String shortName, String longName, String description) {
    return new Option(shortName, longName, true, description);
  }

  /**
   * Helper method to create an Option that requires an argument.
   *
   * @param longName The long option name.
   * @param description The option description.
   * @return The Option object.
   */
  public Option createArgOption(String longName, String description) {
    return new Option(null, longName, true, description);
  }

  /**
   * Helper method to create an Option that requires multiple argument.
   *
   * @param shortName The option name as a single letter
   * @param longName The long option name.
   * @param description The option description.
   * @return The Option object.
   */
  public Option createArgsOption(String shortName, String longName, String description) {
    return Option.builder().option(shortName).longOpt(longName).hasArgs().desc(description).build();
  }
}
