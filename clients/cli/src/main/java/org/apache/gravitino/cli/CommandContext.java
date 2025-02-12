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

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;

/* Context for a command */
public class CommandContext {
  private final boolean force;
  private final boolean ignoreVersions;
  private final String outputFormat;
  private final String url;
  private final CommandLine line;

  private String ignoreEnv;
  private boolean ignoreSet = false;
  private String urlEnv;
  private boolean urlSet = false;
  // Can add more "global" command flags here without any major changes e.g. a guiet flag

  /**
   * Command constructor.
   *
   * @param line The command line.
   */
  public CommandContext(CommandLine line) {
    Preconditions.checkNotNull(line);
    this.line = line;
    this.force = line.hasOption(GravitinoOptions.FORCE);
    this.outputFormat =
        line.hasOption(GravitinoOptions.OUTPUT)
            ? line.getOptionValue(GravitinoOptions.OUTPUT)
            : Command.OUTPUT_FORMAT_PLAIN;

    this.url = getUrl();
    this.ignoreVersions = getIgnore();
  }

  /**
   * Returns the URL.
   *
   * @return The URL.
   */
  public String url() {
    return url;
  }

  /**
   * Indicates whether versions should be ignored.
   *
   * @return False if versions should be ignored.
   */
  public boolean ignoreVersions() {
    return ignoreVersions;
  }

  /**
   * Indicates whether the operation should be forced.
   *
   * @return True if the operation should be forced.
   */
  public boolean force() {
    return force;
  }

  /**
   * Returns the output format.
   *
   * @return The output format.
   */
  public String outputFormat() {
    return outputFormat;
  }

  /**
   * Retrieves the Gravitino URL from the command line options or the GRAVITINO_URL environment
   * variable or the Gravitino config file.
   *
   * @return The Gravitino URL, or null if not found.
   */
  private String getUrl() {
    GravitinoConfig config = new GravitinoConfig(null);

    // If specified on the command line use that
    if (line.hasOption(GravitinoOptions.URL)) {
      return line.getOptionValue(GravitinoOptions.URL);
    }

    // Cache the Gravitino URL environment variable
    if (urlEnv == null && !urlSet) {
      urlEnv = System.getenv("GRAVITINO_URL");
      urlSet = true;
    }

    // If set return the Gravitino URL environment variable
    if (urlEnv != null) {
      return urlEnv;
    }

    // Check if the Gravitino URL is specified in the configuration file
    if (config.fileExists()) {
      config.read();
      String configURL = config.getGravitinoURL();
      if (configURL != null) {
        return configURL;
      }
    }

    // Return the default localhost URL
    return GravitinoCommandLine.DEFAULT_URL;
  }

  private boolean getIgnore() {
    GravitinoConfig config = new GravitinoConfig(null);
    boolean ignore = false;

    /* Check if you should ignore client/version versions */
    if (line.hasOption(GravitinoOptions.IGNORE)) {
      ignore = true;
    } else {
      // Cache the ignore environment variable
      if (ignoreEnv == null && !ignoreSet) {
        ignoreEnv = System.getenv("GRAVITINO_IGNORE");
        ignore = ignoreEnv != null && ignoreEnv.equals("true");
        ignoreSet = true;
      }

      // Check if the ignore name is specified in the configuration file
      if (ignoreEnv == null) {
        if (config.fileExists()) {
          config.read();
          ignore = config.getIgnore();
        }
      }
    }

    return ignore;
  }
}
