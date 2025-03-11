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

/* Gravitino Command line options */
public class GravitinoOptions {
  public static final String AUDIT = "audit";
  public static final String AUTO = "auto";
  public static final String COLUMNFILE = "columnfile";
  public static final String COMMENT = "comment";
  public static final String DATATYPE = "datatype";
  public static final String DEFAULT = "default";
  public static final String DISTRIBUTION = "distribution";
  public static final String FILESET = "fileset";
  public static final String FORCE = "force";
  public static final String GROUP = "group";
  public static final String HELP = "help";
  public static final String IGNORE = "ignore";
  public static final String INDEX = "index";
  public static final String LOGIN = "login";
  public static final String METALAKE = "metalake";
  public static final String NAME = "name";
  public static final String NULL = "null";
  public static final String OUTPUT = "output";
  public static final String OWNER = "owner";
  public static final String PARTITION = "partition";
  public static final String POSITION = "position";
  public static final String PRIVILEGE = "privilege";
  public static final String PROPERTIES = "properties";
  public static final String PROPERTY = "property";
  public static final String QUIET = "quiet";
  public static final String PROVIDER = "provider";
  public static final String RENAME = "rename";
  public static final String ROLE = "role";
  public static final String SERVER = "server";
  public static final String SIMPLE = "simple";
  public static final String SORTORDER = "sortorder";
  public static final String TAG = "tag";
  public static final String URL = "url";
  public static final String USER = "user";
  public static final String VALUE = "value";
  public static final String VERSION = "version";
  public static final String ALL = "all";
  public static final String ENABLE = "enable";
  public static final String DISABLE = "disable";
  public static final String ALIAS = "alias";
  public static final String URI = "uri";

  /**
   * Builds and returns the CLI options for Gravitino.
   *
   * @return Valid CLI command options.
   */
  public Options options() {
    Options options = new Options();

    // Add options using helper method to avoid repetition
    options.addOption(createSimpleOption("h", HELP, "command help information"));
    options.addOption(createSimpleOption("v", VERSION, "Gravitino client version"));
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
    options.addOption(createSimpleOption("o", OWNER, "display entity owner"));
    options.addOption(createArgOption(COLUMNFILE, "CSV file describing columns"));
    options.addOption(createSimpleOption(null, ALL, "on all entities"));

    // model options
    options.addOption(createArgOption(null, URI, "model version artifact"));
    options.addOption(createArgsOption(null, ALIAS, "model aliases"));

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
