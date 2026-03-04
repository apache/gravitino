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

import java.util.Locale;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/** Entry point for the Gravitino command line. */
public class Main {

  /**
   * Flag to indicate whether to use {@link System#exit(int)} or throw a {@link RuntimeException}
   * when exiting the application.
   */
  public static boolean useExit = true;

  /**
   * Main entry point for the Gravitino command line.
   *
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    CommandLineParser parser = new DefaultParser();
    Options options = new GravitinoOptions().options();
    if (args.length == 0) {
      GravitinoCommandLine.displayHelp(options);
      return;
    }

    try {
      CommandLine line = parser.parse(options, args);
      String entity = resolveEntity(line);
      String[] extra = line.getArgs();
      if (extra.length > 2) {
        System.err.println(ErrorMessages.TOO_MANY_ARGUMENTS);
        exit(-1);
      }
      String command = resolveCommand(line);
      GravitinoCommandLine commandLine = new GravitinoCommandLine(line, options, entity, command);

      if (entity != null && command != null) {
        commandLine.handleCommandLine();
      } else {
        commandLine.handleSimpleLine();
      }
    } catch (ParseException exp) {
      System.err.println(ErrorMessages.PARSE_ERROR + exp.getMessage());
      GravitinoCommandLine.displayHelp(options);
      exit(-1);
    }
  }

  /**
   * Exits the application with the specified exit code.
   *
   * <p>If the {@code useExit} flag is set to {@code true}, the application will terminate using
   * {@link System#exit(int)}. Otherwise, a {@link RuntimeException} is thrown with a message
   * containing the exit code. Helps with testing.
   *
   * @param code the exit code to use when exiting the application
   */
  public static void exit(int code) {
    if (useExit) {
      System.exit(code);
    } else {
      throw new RuntimeException("Exit with code " + code);
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
      String action = args[1].toLowerCase(Locale.ENGLISH);
      if (CommandActions.isValidCommand(action)) {
        return action;
      }
    } else if (args.length == 1) {
      /* Default to 'details' command. */
      return line.hasOption(GravitinoOptions.HELP) ? CommandActions.HELP : CommandActions.DETAILS;
    } else if (args.length == 0) {
      return null;
    }

    System.err.println(ErrorMessages.UNSUPPORTED_COMMAND);
    exit(-1);
    return null; // not needed but gives error if not here
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
      String entity = args[0].toLowerCase(Locale.ENGLISH);
      if (CommandEntities.isValidEntity(entity)) {
        return entity;
      } else {
        System.err.println(ErrorMessages.UNKNOWN_ENTITY);
        exit(-1);
      }
    }

    return null;
  }
}
