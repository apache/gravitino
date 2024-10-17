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

import org.apache.commons.cli.CommandLine;

/**
 * Extracts different arts of a full name (dot seperated) from the command line input. This includes
 * metalake, catalog, schema, and table names.
 */
public class FullName {
  private CommandLine line;
  private String metalakeEnv;

  /**
   * Constructor for the {@code FullName} class.
   *
   * @param line The parsed command line arguments.
   */
  public FullName(CommandLine line) {
    this.line = line;
  }

  /**
   * Retrieves the metalake name from the command line options, environment variables, or the first
   * part of the full name.
   *
   * @return The metalake name, or null if not found.
   */
  public String getMetalakeName() {
    GravitinoConfig config = new GravitinoConfig(null);

    // Cache the metalake environment variable
    if (metalakeEnv == null) {
      metalakeEnv = System.getenv("GRAVITINO_METALAKE");
    }

    // Check if the metalake name is set as an environment variable
    if (metalakeEnv != null) {
      return metalakeEnv;
      // Check if the metalake name is specified in the configuration file
    } else if (config.fileExists()) {
      config.read();
      String configName = config.getMetalakeName();
      if (configName != null) {
        return config.getMetalakeName();
      }
    }

    // Extract the metalake name from the full name option
    if (line.hasOption(GravitinoOptions.NAME)) {
      return line.getOptionValue(GravitinoOptions.NAME).split("\\.")[0];
    }

    return null;
  }

  /**
   * Retrieves the catalog name from the second part of the full name option.
   *
   * @return The catalog name, or null if not found.
   */
  public String getCatalogName() {
    return getNamePart(1);
  }

  /**
   * Retrieves the schema name from the third part of the full name option.
   *
   * @return The schema name, or null if not found.
   */
  public String getSchemaName() {
    return getNamePart(2);
  }

  /**
   * Retrieves the table name from the fourth part of the full name option.
   *
   * @return The table name, or null if not found.
   */
  public String getTableName() {
    return getNamePart(3);
  }

  /**
   * Helper method to retrieve a specific part of the full name based on the position of the part.
   *
   * @param position The position of the name part in the full name string.
   * @return The extracted part of the name, or {@code null} if the name part is missing or
   *     malformed.
   */
  public String getNamePart(int position) {
    /* Extract the name part from the full name if available. */
    if (line.hasOption(GravitinoOptions.NAME)) {
      String[] names = line.getOptionValue(GravitinoOptions.NAME).split("\\.");

      /* Adjust position if metalake is part of the full name. */
      if (metalakeEnv != null) {
        position = position - 1;
      }

      if (position >= names.length) {
        System.err.println(ErrorMessages.MALFORMED_NAME);
        return null;
      }

      return names[position];
    }

    System.err.println(ErrorMessages.MISSING_NAME);
    return null;
  }

  /**
   * Helper method to determine a specific part of the full name exits.
   *
   * @param partNo The part of the name to obtain.
   * @return True if the part exitsts.
   */
  public boolean hasNamePart(int partNo) {
    /* Extract the name part from the full name if available. */
    if (line.hasOption(GravitinoOptions.NAME)) {
      String[] names = line.getOptionValue(GravitinoOptions.NAME).split("\\.");
      int length = names.length;
      int position = partNo;

      /* Adjust position if metalake is part of the full name. */
      if (metalakeEnv != null) {
        position = position - 1;
      }
      return position <= length;
    }

    return false;
  }

  /**
   * Does the metalake name exist?
   *
   * @return True if the catalog name exists, or false if it does not.
   */
  public boolean hasMetalakeName() {
    return hasNamePart(1);
  }

  /**
   * Does the catalog name exist?
   *
   * @return True if the catalog name exists, or false if it does not.
   */
  public boolean hasCatalogName() {
    return hasNamePart(2);
  }

  /**
   * Does the schema name exist?
   *
   * @return True if the schema name exists, or false if it does not.
   */
  public boolean hasSchemaName() {
    return hasNamePart(3);
  }

  /**
   * Does the table name exist?
   *
   * @return True if the table name exists, or false if it does not.
   */
  public boolean hasTableName() {
    return hasNamePart(4);
  }
}
