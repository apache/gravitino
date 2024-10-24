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

public class TestCommandEntities {

  @Test
  public void validEntities() {
    assertTrue(
        CommandEntities.isValidEntity(CommandEntities.METALAKE),
        "METALAKE should be a valid entity");
    assertTrue(
        CommandEntities.isValidEntity(CommandEntities.CATALOG), "CATALOG should be a valid entity");
    assertTrue(
        CommandEntities.isValidEntity(CommandEntities.SCHEMA), "SCHEMA should be a valid entity");
    assertTrue(
        CommandEntities.isValidEntity(CommandEntities.TABLE), "TABLE should be a valid entity");
  }

  @Test
  public void invalidEntity() {
    assertFalse(
        CommandEntities.isValidEntity("invalidEntity"), "An invalid command should return false");
  }

  @Test
  public void nullEntity() {
    assertFalse(
        CommandEntities.isValidEntity(null), "Null should return false as it's not a valid entity");
  }

  @Test
  public void emptyEntity() {
    assertFalse(
        CommandEntities.isValidEntity(""),
        "Empty string should return false as it's not a valid entity");
  }

  @Test
  public void caseSensitive() {
    assertFalse(CommandEntities.isValidEntity("METALAKE"), "Entities should be case-sensitive");
  }
}
