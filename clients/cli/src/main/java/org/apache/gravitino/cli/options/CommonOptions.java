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

package org.apache.gravitino.cli.options;

import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.outputs.OutputFormat;
import picocli.CommandLine;

/** Common options for all commands. */
public class CommonOptions {

  /** display help message, use --help/-h to display help message */
  @CommandLine.Option(
      names = {"-h", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP},
      usageHelp = true,
      description = "display help message")
  public boolean usageHelpRequested;

  /** whether versions should be ignored, use --ignore to ignore versions */
  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.IGNORE},
      description = "Whether versions should be ignored",
      negatable = true,
      defaultValue = "false")
  public boolean ignoreVersions;

  /**
   * whether command information should be suppressed, use --quiet to suppress command information
   */
  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.QUIET},
      description = "Whether command information should be suppressed.",
      negatable = true,
      defaultValue = "false")
  public boolean quiet;

  /** name of the metalake, use --metalake/-m to specify the metalake */
  @CommandLine.Option(
      names = {"-m", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.METALAKE},
      description = "name of the metalake")
  public String metalake;

  /** output format, use --output to specify the output format */
  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.OUTPUT},
      description = "The format of output. Valid values: ${COMPLETION-CANDIDATES}",
      defaultValue = "PLAIN")
  public OutputFormat.OutputType outputFormat;

  /** Gravitino URL, use --url/-u to specify the Gravitino URL */
  @CommandLine.Option(
      names = {"-u", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.URL},
      description = "Gravitino URL (default: http://localhost:8090)",
      defaultValue = "http://localhost:8090")
  public String url;

  /** Login for authentication, use --login to specify the login for authentication */
  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.LOGIN},
      description = "Login for authentication")
  public String login;

  /** Whether to use simple authentication, use --simple to use simple authentication */
  @CommandLine.Option(
      names = {GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.SIMPLE},
      description = "simple authentication")
  public boolean simple;
}
