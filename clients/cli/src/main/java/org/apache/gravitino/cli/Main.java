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
      String[] extra = line.getArgs();
      if (extra.length > 2) {
        System.err.println(ErrorMessages.TOO_MANY_ARGUMENTS);
        return;
      }
      String command = resolveCommand(line);
      String entity = resolveEntity(line, command != null);
      GravitinoCommandLine commandLine = new GravitinoCommandLine(line, options, entity, command);
      commandLine.handleCommandLine();
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
    /* Can be specified in the form --command XXX. */
    if (line.hasOption(GravitinoOptions.COMMAND)) {
      String command = line.getOptionValue(GravitinoOptions.COMMAND);
      if (CommandActions.isValidCommand(command)) {
        return command;
      }
    }

    /* Or as --list, --details --create --delete or --update. */
    if (line.hasOption(GravitinoOptions.LIST)) {
      return CommandActions.LIST;
    } else if (line.hasOption(GravitinoOptions.DETAILS)) {
      return CommandActions.DETAILS;
    } else if (line.hasOption(GravitinoOptions.CREATE)) {
      return CommandActions.CREATE;
    } else if (line.hasOption(GravitinoOptions.DELETE)) {
      return CommandActions.DELETE;
    } else if (line.hasOption(GravitinoOptions.UPDATE)) {
      return CommandActions.UPDATE;
    }

    /* Or as the first command or bare second argument of two arguments. */
    String[] args = line.getArgs();

    if (args.length == 1 || args.length == 2) {
      String command = args[args.length - 1];
      if (CommandActions.isValidCommand(command)) {
        return command;
      } else {
        System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
        return null;
      }
    } else {
      return CommandActions.DETAILS; /* Default to 'details' command. */
    }
  }

  /**
   * Determines the entity to act upon based on the command line input.
   *
   * @param line Parsed command line object.
   * @param command true if command is an argument
   * @return The entity, e.g. metakalake, catalog, schema, table, etc.
   */
  protected static String resolveEntity(CommandLine line, boolean command) {
    /* Can be specified in the form --entity XXX. */
    if (line.hasOption(GravitinoOptions.ENTITY)) {
      String entity = line.getOptionValue(GravitinoOptions.ENTITY);
      if (CommandEntities.isValidEntity(entity)) {
        return entity;
      }
    }

    /* Or as --metalake, --catalog, --schema, --table etc. */
    if (line.hasOption(GravitinoOptions.TABLE)) {
      return CommandEntities.TABLE;
    } else if (line.hasOption(GravitinoOptions.SCHEMA)) {
      return CommandEntities.SCHEMA;
    } else if (line.hasOption(GravitinoOptions.CATALOG)) {
      return CommandEntities.CATALOG;
    } else if (line.hasOption(GravitinoOptions.METALAKE)) {
      return CommandEntities.METALAKE;
    }

    /* Or as the bare first argument of one or two arguments. */
    String[] args = line.getArgs();
    String entity = args[0];

    if (args.length == 1) {
      if (CommandActions.isValidCommand(args[0])) {
        return null; /* But not an error. */
      }
    }

    if (CommandEntities.isValidEntity(entity)) {
      return entity;
    } else {
      System.err.println(ErrorMessages.UNKNOWN_ENTITY);
    }

    return null;
  }
}
