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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.commands.Command;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCommandContext {

  private CommandLine mockCommandLine;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
  }

  @Test
  public void testCreateCommandContextWithDefaults() {
    CommandContext commandContext = new CommandContext(mockCommandLine);

    Assertions.assertEquals(GravitinoCommandLine.DEFAULT_URL, commandContext.url());
    Assertions.assertFalse(commandContext.ignoreVersions());
    Assertions.assertFalse(commandContext.force());
    Assertions.assertEquals(Command.OUTPUT_FORMAT_PLAIN, commandContext.outputFormat());
  }

  @Test
  public void testCreateCommandContextWithCustomValues() {
    when(mockCommandLine.hasOption(GravitinoOptions.URL)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.URL)).thenReturn("http://localhost:8090");
    when(mockCommandLine.hasOption(GravitinoOptions.IGNORE)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.FORCE)).thenReturn(true);
    when(mockCommandLine.hasOption(GravitinoOptions.OUTPUT)).thenReturn(true);
    when(mockCommandLine.getOptionValue(GravitinoOptions.OUTPUT))
        .thenReturn(Command.OUTPUT_FORMAT_TABLE);

    CommandContext commandContext = new CommandContext(mockCommandLine);
    Assertions.assertEquals("http://localhost:8090", commandContext.url());
    Assertions.assertTrue(commandContext.ignoreVersions());
    Assertions.assertTrue(commandContext.force());
    Assertions.assertEquals(Command.OUTPUT_FORMAT_TABLE, commandContext.outputFormat());
  }

  @Test
  public void testCreateCommandContextWithNull() {
    Assertions.assertThrows(NullPointerException.class, () -> new CommandContext(null));
  }
}
