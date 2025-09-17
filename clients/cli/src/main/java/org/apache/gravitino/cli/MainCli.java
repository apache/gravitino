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

import org.apache.gravitino.cli.handler.CatalogCliHandler;
import org.apache.gravitino.cli.handler.MetalakeCliHandler;
import org.apache.gravitino.cli.handler.ModelCliHandler;
import org.apache.gravitino.cli.handler.SchemaCliHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Gravitino CLI */
@Command(
    name = "gravitino",
    description = "Gravitino CLI",
    mixinStandardHelpOptions = true,
    subcommands = {
      ModelCliHandler.class,
      CatalogCliHandler.class,
      SchemaCliHandler.class,
      MetalakeCliHandler.class
    })
public class MainCli implements Runnable {

  /** Flag to indicate if System. Exit should be used instead of throwing an exception. */
  public static boolean useExit = true;

  /** Run the CLI. */
  @Override
  public void run() {
    System.out.println("Use <entity> <command> [options]");
  }

  /**
   * Main method to start the CLI.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    int exitCode = new CommandLine(new MainCli()).execute(args);
    exit(exitCode);
  }

  /**
   * Exit the CLI with the specified code.
   *
   * @param code exit code
   */
  public static void exit(int code) {
    if (code == 0) {
      return;
    }
    if (useExit) {
      System.exit(code);
    } else {
      throw new RuntimeException("Exit with code " + code);
    }
  }
}
