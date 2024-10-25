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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestCommandActions {

  @Test
  public void validCommands() {
    assertTrue(
        CommandActions.isValidCommand(CommandActions.DETAILS), "DETAILS should be a valid command");
    assertTrue(
        CommandActions.isValidCommand(CommandActions.LIST), "LIST should be a valid command");
    assertTrue(
        CommandActions.isValidCommand(CommandActions.UPDATE), "UPDATE should be a valid command");
    assertTrue(
        CommandActions.isValidCommand(CommandActions.CREATE), "CREATE should be a valid command");
    assertTrue(
        CommandActions.isValidCommand(CommandActions.DELETE), "DELETE should be a valid command");
    assertTrue(CommandActions.isValidCommand(CommandActions.SET), "SET should be a valid command");
    assertTrue(
        CommandActions.isValidCommand(CommandActions.REMOVE), "REMOVE should be a valid command");
    assertTrue(
        CommandActions.isValidCommand(CommandActions.PROPERTIES),
        "PROPERTIES should be a valid command");
  }

  @Test
  public void invalidCommand() {
    assertFalse(
        CommandActions.isValidCommand("invalidCommand"), "An invalid command should return false");
  }

  @Test
  public void nullCommand() {
    assertFalse(
        CommandActions.isValidCommand(null),
        "Null should return false as it's not a valid command");
  }

  @Test
  public void emptyCommand() {
    assertFalse(
        CommandActions.isValidCommand(""),
        "Empty string should return false as it's not a valid command");
  }

  @Test
  public void caseSensitive() {
    assertFalse(CommandActions.isValidCommand("DETAILS"), "Commands should be case-sensitive");
  }
}
