/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.cli.commands;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.InUseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeleteRole {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setup() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  void restore() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    Main.useExit = true;
  }

  @Test
  void referencedRoleDoesNotStopTheRemainingDeletions() {
    Main.useExit = false;
    CommandLine mockCmdLine = mock(CommandLine.class);
    when(mockCmdLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    CommandContext context = new CommandContext(mockCmdLine);

    DeleteRole command =
        spy(new DeleteRole(context, "metalake1", new String[] {"referenced", "unused"}));
    GravitinoClient mockClient = mock(GravitinoClient.class);
    doReturn(mockClient).when(command).buildClient("metalake1");
    doThrow(new InUseException("Role referenced is referenced by access control policies"))
        .when(mockClient)
        .deleteRole("referenced");
    doReturn(true).when(mockClient).deleteRole("unused");

    RuntimeException ex = assertThrows(RuntimeException.class, command::handle);

    assertTrue(ex.getMessage().contains("Exit with code -1"));
    verify(mockClient).deleteRole("unused");
    assertTrue(outContent.toString(UTF_8).contains("unused deleted."));
    assertTrue(
        errContent.toString(UTF_8).contains("referenced is referenced by access control policies"));
  }
}
