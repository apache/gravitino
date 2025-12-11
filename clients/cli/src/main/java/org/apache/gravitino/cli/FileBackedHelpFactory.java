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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import picocli.CommandLine;

/** A Help Factory that loads description text from a file. */
public class FileBackedHelpFactory implements CommandLine.IHelpFactory {
  private static final CommandLine.IHelpFactory DEFAULT_FACTORY = CommandLine.Help::new;

  /** {@inheritDoc} */
  @Override
  public CommandLine.Help create(
      CommandLine.Model.CommandSpec commandSpec, CommandLine.Help.ColorScheme colorScheme) {
    CommandLine.Help help = DEFAULT_FACTORY.create(commandSpec, colorScheme);
    String[] name = commandSpec.qualifiedName(" ").split(" ");

    if (name.length < 3) {
      return help;
    }

    String fileName = "description/" + name[1] + "_" + name[2] + "_description.txt";
    String desc = loadFromResource(fileName);

    if (!desc.isEmpty()) {
      commandSpec.usageMessage().description(desc);
    }

    return help;
  }

  private String loadFromResource(String path) {
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(path)),
                StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append(System.lineSeparator());
      }
      return sb.toString();
    } catch (Exception e) {
      System.err.println(ErrorMessages.HELP_FAILED + e.getMessage());
    }
    return "";
  }
}
