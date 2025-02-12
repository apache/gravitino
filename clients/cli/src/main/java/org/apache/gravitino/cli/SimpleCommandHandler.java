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

/** Handles the command execution for simple command based on the command line options. */
public class SimpleCommandHandler extends CommandHandler {
  private final GravitinoCommandLine gravitinoCommandLine;
  private final CommandLine line;
  private final CommandContext context;

  /**
   * Constructs a {@link SimpleCommandHandler} instance.
   *
   * @param gravitinoCommandLine The Gravitino command line instance.
   * @param line The command line arguments.
   * @param context The command context.
   */
  public SimpleCommandHandler(
      GravitinoCommandLine gravitinoCommandLine, CommandLine line, CommandContext context) {
    this.gravitinoCommandLine = gravitinoCommandLine;
    this.line = line;
    this.context = context;
  }

  /** Handles the command execution logic based on the provided command. */
  @Override
  protected void handle() {
    if (line.hasOption(GravitinoOptions.VERSION)) {
      gravitinoCommandLine.newClientVersion(context).validate().handle();
    } else if (line.hasOption(GravitinoOptions.SERVER)) {
      gravitinoCommandLine.newServerVersion(context).validate().handle();
    }
  }
}
