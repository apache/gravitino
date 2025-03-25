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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.gravitino.cli.commands.ClientVersion;
import org.apache.gravitino.cli.commands.ServerVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSimpleCommands {

  private CommandLine mockCommandLine;
  private Options mockOptions;

  @BeforeEach
  void setUp() {
    mockCommandLine = mock(CommandLine.class);
    mockOptions = mock(Options.class);
  }

  @Test
  void testServerVersion() {
    ServerVersion mockServerVersion = mock(ServerVersion.class);
    when(mockCommandLine.hasOption(GravitinoOptions.SERVER)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(new GravitinoCommandLine(mockCommandLine, mockOptions, null, null));

    doReturn(mockServerVersion).when(commandLine).newServerVersion(any(CommandContext.class));
    doReturn(mockServerVersion).when(mockServerVersion).validate();
    commandLine.handleSimpleLine();
    verify(mockServerVersion).handle();
  }

  @Test
  void testClientVersion() {
    ClientVersion mockClientVersion = mock(ClientVersion.class);
    when(mockCommandLine.hasOption(GravitinoOptions.VERSION)).thenReturn(true);
    GravitinoCommandLine commandLine =
        spy(new GravitinoCommandLine(mockCommandLine, mockOptions, null, null));

    doReturn(mockClientVersion).when(commandLine).newClientVersion(any(CommandContext.class));
    doReturn(mockClientVersion).when(mockClientVersion).validate();
    commandLine.handleSimpleLine();
    verify(mockClientVersion).handle();
  }
}
