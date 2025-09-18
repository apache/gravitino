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

import java.util.regex.Pattern;
import picocli.CommandLine;

/**
 * Extracts different parts of a full name (dot separated) from the command line input. This
 * includes metalake, catalog, schema, and table names.
 */
public class CliFullName {
  private String metalakeEnv;
  private boolean metalakeSet = false;
  private boolean hasDisplayedMissingNameInfo = true;
  private boolean hasDisplayedMalformedInfo = true;
  private String fullName;
  private CommandLine.Model.CommandSpec spec;
  private String[] names;

  /**
   * Construct a new instance of the {@link CliFullName} class.
   *
   * @param spec The command line specification.
   */
  public CliFullName(picocli.CommandLine.Model.CommandSpec spec) {
    this.spec = spec;
    this.fullName = getFullName();
    this.names = fullName.split(Pattern.quote("."));
  }

  /**
   * Retrieves the level of the full name.
   *
   * @return The level of the full name, or -1 if line does not contain a {@code --name} option.
   */
  public int getLevel() {
    if (fullName != null) {
      String[] names = fullName.split(Pattern.quote("."));
      return names.length;
    }

    return -1;
  }

  /**
   * Retrieves the metalake name from the command line options, the GRAVITINO_METALAKE environment
   * variable or the Gravitino config file.
   *
   * @return The metalake name, or null if not found.
   */
  public String getMetalakeName() {
    GravitinoConfig config = new GravitinoConfig(null);

    // If specified on the command line use that
    if (spec.findOption(GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.METALAKE) != null) {
      return getMetalake();
    }

    // Cache the metalake environment variable
    if (metalakeEnv == null && !metalakeSet) {
      metalakeEnv = System.getenv("GRAVITINO_METALAKE");
      metalakeSet = true;
    }

    // Check if the metalake name is set as an environment variable
    if (metalakeEnv != null) {
      return metalakeEnv;
      // Check if the metalake name is specified in the configuration file
    } else if (config.fileExists()) {
      config.read();
      String configName = config.getMetalakeName();
      if (configName != null) {
        return configName;
      }
    }

    System.err.println(ErrorMessages.MISSING_METALAKE);
    MainCli.exit(-1);

    return null;
  }

  /**
   * Retrieves the catalog name from the first part of the full name option.
   *
   * @return the catalog name, or null if not found.
   */
  public String getCatalogName() {
    return getNamePart(0);
  }

  /**
   * Retrieves the schema name from the second part of the full name option.
   *
   * @return The schema name, or null if not found.
   */
  public String getSchemaName() {
    return getNamePart(1);
  }

  /**
   * Retrieves the model name from the second part of the full name option.
   *
   * @return The model name, or null if not found.
   */
  public String getModelName() {
    return getNamePart(2);
  }

  /**
   * Retrieves the table name from the third part of the full name option.
   *
   * @return The table name, or null if not found.
   */
  public String getTableName() {
    return getNamePart(2);
  }

  /**
   * Retrieves the topic name from the third part of the full name option.
   *
   * @return The topic name, or null if not found.
   */
  public String getTopicName() {
    return getNamePart(2);
  }

  /**
   * Retrieves the fileset name from the third part of the full name option.
   *
   * @return The fileset name, or null if not found.
   */
  public String getFilesetName() {
    return getNamePart(2);
  }

  /**
   * Retrieves the column name from the fourth part of the full name option.
   *
   * @return The column name, or null if not found.
   */
  public String getColumnName() {
    return getNamePart(3);
  }

  /**
   * Retrieves the name part from the full name option.
   *
   * @param position The index of the name part to retrieve.
   * @return The name part, or null if not found.
   */
  public String getNamePart(int position) {
    if (fullName.isEmpty()) {
      showMissingNameInfo();
      return null;
    }

    /* Extract the name part from the full name if available. */
    if (names.length <= position) {
      showMalformedInfo();
      return null;
    }

    return names[position];
  }

  /**
   * Does the catalog name exist?
   *
   * @return True if the catalog name exists, or false if it does not.
   */
  public boolean hasCatalogName() {
    return hasNamePart(1);
  }

  /**
   * Does the schema name exist?
   *
   * @return True if the schema name exists, or false if it does not.
   */
  public boolean hasSchemaName() {
    return hasNamePart(2);
  }

  /**
   * Does the table name exist?
   *
   * @return True if the table name exists, or false if it does not.
   */
  public boolean hasTableName() {
    return hasNamePart(3);
  }

  /**
   * Does the column name exist?
   *
   * @return True if the column name exists, or false if it does not.
   */
  public boolean hasColumnName() {
    return hasNamePart(4);
  }

  /**
   * Helper method to determine a specific part of the full name exits.
   *
   * @param partNo The part of the name to obtain.
   * @return True if the part exists.
   */
  public boolean hasNamePart(int partNo) {
    if (fullName.isEmpty()) {
      return false;
    }

    return partNo <= names.length;
  }

  /**
   * Retrieves the name from the command line options.
   *
   * @return The name, or null if not found.
   */
  public String getName() {
    return fullName;
  }

  /**
   * Are there any names that can be retrieved?
   *
   * @return True if the name exists, or false if it does not.
   */
  public Boolean hasName() {
    return !fullName.isEmpty();
  }

  private String getFullName() {
    CommandLine.Model.OptionSpec option =
        spec.findOption(GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.NAME);

    if (option == null || option.getValue() == null) {
      return "";
    }

    return option.getValue();
  }

  private String getMetalake() {
    CommandLine.Model.OptionSpec option =
        spec.findOption(GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.METALAKE);
    if (option == null) {
      return "";
    }

    return option.getValue();
  }

  private void showMissingNameInfo() {
    if (hasDisplayedMissingNameInfo) {
      System.err.println(ErrorMessages.MISSING_NAME);
      hasDisplayedMissingNameInfo = false;
    }
  }

  private void showMalformedInfo() {
    if (hasDisplayedMalformedInfo) {
      System.err.println(ErrorMessages.MALFORMED_NAME);
      hasDisplayedMalformedInfo = false;
    }
  }
}
