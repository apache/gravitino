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

import java.util.HashSet;

/**
 * The {@code CommandActions} class defines a set of standard commands that can be used in the
 * Gravitino CLI. It also can validate if a given command is a valid commands.
 */
public class CommandActions {
  public static final String DETAILS = "details";
  public static final String LIST = "list";
  public static final String UPDATE = "update";
  public static final String CREATE = "create";
  public static final String DELETE = "delete";

  private static final HashSet<String> VALID_COMMANDS = new HashSet<>();

  static {
    VALID_COMMANDS.add(DETAILS);
    VALID_COMMANDS.add(LIST);
    VALID_COMMANDS.add(UPDATE);
    VALID_COMMANDS.add(CREATE);
    VALID_COMMANDS.add(DELETE);
  }

  /**
   * Checks if a given command is a valid command type.
   *
   * @param command The command to check.
   * @return true if the command is valid, false otherwise.
   */
  public static boolean isValidCommand(String command) {
    return VALID_COMMANDS.contains(command);
  }
}
