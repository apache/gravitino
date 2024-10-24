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
  public static final String HELP = "help";
  public static final String VERSION = "version";
  public static final String SERVER = "server";
  public static final String URL = "url";
  public static final String NAME = "name";
  public static final String METALAKE = "metalake";
  public static final String IGNORE = "ignore";

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
    options.addOption(createSimpleOption("r", SERVER, "Gravitino server version"));
    options.addOption(createArgOption("u", URL, "Gravitino URL (default: http://localhost:8090)"));
    options.addOption(createArgOption("f", NAME, "full entity name (dot separated)"));
    options.addOption(createArgOption("m", METALAKE, "Metalake name"));
    options.addOption(createSimpleOption("i", IGNORE, "Ignore client/sever version check"));

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
}
