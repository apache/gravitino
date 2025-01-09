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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCommandHandler {
  private CommandLine mockCommandLine;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testGetUrlFromLine() {
    when(mockCommandLine.hasOption("url")).thenReturn(true);
    when(mockCommandLine.getOptionValue("url")).thenReturn("http://localhost:8080");
    CommandHandler commandHandler =
        new CommandHandler() {
          @Override
          protected void handle() {}
        };

    String url = commandHandler.getUrl(mockCommandLine);
    assertEquals("http://localhost:8080", url);
  }

  @Test
  void testGetDefaultUrl() {
    when(mockCommandLine.hasOption("url")).thenReturn(false);
    CommandHandler commandHandler =
        new CommandHandler() {
          @Override
          protected void handle() {}
        };
    String url = commandHandler.getUrl(mockCommandLine);
    assertEquals("http://localhost:8090", url);
  }
}
