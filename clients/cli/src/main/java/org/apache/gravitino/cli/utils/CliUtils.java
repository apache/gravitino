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

package org.apache.gravitino.cli.utils;

import static org.apache.gravitino.cli.TableCommandHandler.cliOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.gravitino.cli.CliOptions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.CommandHandler;
import org.apache.gravitino.cli.GravitinoCommandLine;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.cli.MetalakeCommandHandler;

/** This class contains utility methods for the CLI. */
public class CliUtils {

  /**
   * Prints the help message for the CLI.
   *
   * <p>The help message includes a list of all available entities, as well as a brief description
   * of each common option.
   *
   * <p>The help message is printed to the console.
   */
  public static void printHelp() {
    System.out.println();
    System.out.println(
        "Usage: ./bin/gcli.sh [gravitino-options] <entity> <command> [command-options]");
    System.out.printf("Entities: %s\n", String.join(", ", CommandEntities.VALID_ENTITIES));
    System.out.println("Gravitino Options:");
    for (GravitinoOptions.CommandOptions commonOption : CommandHandler.COMMON_OPTIONS) {
      System.out.printf("  --%s %s\n", commonOption.getValue(), commonOption.getDescription());
    }

    System.out.println(
        "For detailed help on entity specific operations, use ./bin/gcli.sh <entity> --help");
  }

  /**
   * Prints the help message for an entity.
   *
   * @param entity The entity for which the help message is to be printed.
   * @param verbose The verbose flag. If true, the help message includes an example of the entity.
   */
  public static void printEntityHelp(String entity, boolean verbose) {
    // TODO: Implement help for each entity
    System.out.println(
        "Usage: ./bin/gcli.sh " + "[gravitino-options] " + entity + " <command> [command-options]");
    System.out.println("Commands:");
    switch (entity) {
      case CommandEntities.METALAKE:
        MetalakeCommandHandler.cliOptions
            .keySet()
            .forEach(cmd -> System.out.printf("  -- %s\n", cmd));
        break;

      case CommandEntities.TABLE:
        cliOptions.keySet().forEach(cmd -> System.out.printf("  -- %s\n", cmd));
        break;

      default:
        break;
    }
    System.out.println(
        "For detailed help on sub commands specific operations, use ./bin/gcli.sh "
            + entity
            + " <command>"
            + " "
            + "--help");

    System.out.println();
    if (verbose) {
      disPlayEntityExample(entity);
    }
  }

  /**
   * Prints the help message for a sub command.
   *
   * @param entity The entity for which the sub command belongs to.
   * @param command The sub command for which the help message is to be printed.
   */
  public static void printSubCommandHelp(String entity, String command) {
    switch (entity) {
      case CommandEntities.METALAKE:
        printSubCommandHelp(entity, command, MetalakeCommandHandler.cliOptions.get(command));
        break;

      case CommandEntities.TABLE:
        printSubCommandHelp(entity, command, cliOptions.get(command));
        break;

      default:
        break;
    }
  }

  private static void printSubCommandHelp(String entity, String command, CliOptions options) {
    System.out.printf(
        "Usage: ./bin/gcli.sh [gravitino-options] %s %s <required args> [optional args]\n",
        entity, command);
    System.out.println("Required Args:");
    printSection(options.getNecessaryParams());

    System.out.println("Optional Args:");
    printSection(options.getOptionalParams());

    System.out.println("Gravitino Options:");
    printSection(CommandHandler.COMMON_OPTIONS);
  }

  private static void printSection(List<GravitinoOptions.CommandOptions> options) {
    if (options.isEmpty()) {
      System.out.print("  N/A\n");
    } else {
      options.forEach(
          option -> System.out.printf("  --%s: %s\n", option.getValue(), option.getDescription()));
    }
  }

  private static void disPlayEntityExample(String entity) {
    String helpFile = entity.toLowerCase() + "_help.txt";

    try (InputStream inputStream =
            GravitinoCommandLine.class.getClassLoader().getResourceAsStream(helpFile);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

      StringBuilder helpMessage = new StringBuilder();
      String helpLine;
      while ((helpLine = reader.readLine()) != null) {
        helpMessage.append(helpLine).append(System.lineSeparator());
      }
      System.out.print(helpMessage.toString());
    } catch (IOException e) {
      System.err.println("Failed to load help for entity '" + entity + "': " + e.getMessage());
      Main.exit(-1);
    }
  }
}
