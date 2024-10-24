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
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/* Entry point for teh Gravitino command line. */
public class Main {

  public static void main(String[] args) {
    CommandLineParser parser = new DefaultParser();
    Options options = new GravitinoOptions().options();

    try {
      CommandLine line = parser.parse(options, args);
      String entity = resolveEntity(line);
      String[] extra = line.getArgs();
      if (extra.length > 2) {
        System.err.println(ErrorMessages.TOO_MANY_ARGUMENTS);
        return;
      }
      String command = resolveCommand(line);
      GravitinoCommandLine commandLine = new GravitinoCommandLine(line, options, entity, command);

      if (entity != null && command != null) {
        commandLine.handleCommandLine();
      } else {
        commandLine.handleSimpleLine();
      }
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      GravitinoCommandLine.displayHelp(options);
    }
  }

  /**
   * Determines the command based on the command line input.
   *
   * @param line Parsed command line object.
   * @return The command, one of 'details', 'list', 'create', 'delete' or 'update'.
   */
  protected static String resolveCommand(CommandLine line) {

    /* As the bare second argument. */
    String[] args = line.getArgs();

    if (args.length == 2) {
      String action = args[1];
      if (CommandActions.isValidCommand(action)) {
        return action;
      }
    } else if (args.length == 1) {
      return CommandActions.DETAILS; /* Default to 'details' command. */
    } else if (args.length == 0) {
      return null;
    }

    System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
    return null;
  }

  /**
   * Determines the entity to act upon based on the command line input.
   *
   * @param line Parsed command line object.
   * @return The entity, e.g. metakalake, catalog, schema, table, etc.
   */
  protected static String resolveEntity(CommandLine line) {
    /* As the bare first argument. */
    String[] args = line.getArgs();

    if (args.length >= 1) {
      String entity = args[0];
      if (CommandEntities.isValidEntity(entity)) {
        return entity;
      } else {
        System.err.println(ErrorMessages.UNKNOWN_ENTITY);
        return null;
      }
    }

    return null;
  }
}
