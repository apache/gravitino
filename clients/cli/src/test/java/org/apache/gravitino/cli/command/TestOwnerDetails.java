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
package org.apache.gravitino.cli.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.cli.commands.Command;
import org.apache.gravitino.cli.commands.OwnerDetails;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestOwnerDetails {

  private CommandContext context;

  @Before
  public void setUp() {
    CommandLine mockCommandLine = Mockito.mock(CommandLine.class);
    context = new CommandContext(mockCommandLine);
    Main.useExit = false;
  }

  @After
  public void tearDown() {
    Main.useExit = true;
  }

  @Test
  public void testValidate_withValidEntityType_shouldPass() {
    OwnerDetails command = new OwnerDetails(context, "metalake1", "entity1", CommandEntities.TABLE);
    Command result = command.validate();
    assertNotNull(result);
    assertEquals(command, result);
  }

  @Test
  public void testValidate_withInvalidEntityType_shouldExit() {
    OwnerDetails command = new OwnerDetails(context, "metalake1", "entity1", "INVALID_TYPE");

    try {
      command.validate();
      fail("Expected RuntimeException due to intercepted exit");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Exit with code"));
    }
  }
}
