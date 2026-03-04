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

package org.apache.gravitino.cli.commands;

import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelVersionChange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class TestUpdateModelVersionAliases {

  @BeforeEach
  void setUp() {
    Main.useExit = false;
  }

  @AfterEach
  void tearDown() {
    Main.useExit = true;
  }

  @Test
  void handleWithAliasCallsAlterModelVersion() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionAliases command =
        Mockito.spy(
            new UpdateModelVersionAliases(
                context,
                "metalake1",
                "catalog1",
                "schema1",
                "model1",
                null,
                "alias1",
                new String[] {"add1", "add2"},
                new String[] {"remove1"}));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);

    command.handle();

    Mockito.verify(mockModelCatalog)
        .alterModelVersion(
            ArgumentMatchers.any(NameIdentifier.class),
            ArgumentMatchers.eq("alias1"),
            ArgumentMatchers.any(ModelVersionChange.class));
    Mockito.verify(command)
        .printInformation(ArgumentMatchers.contains("alias alias1 aliases changed."));
  }

  @Test
  void handleWithVersionCallsAlterModelVersion() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionAliases command =
        Mockito.spy(
            new UpdateModelVersionAliases(
                context,
                "metalake1",
                "catalog1",
                "schema1",
                "model1",
                99,
                null,
                new String[] {"add1"},
                new String[] {"remove1"}));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);

    command.handle();

    Mockito.verify(mockModelCatalog)
        .alterModelVersion(
            ArgumentMatchers.any(NameIdentifier.class),
            ArgumentMatchers.eq(99),
            ArgumentMatchers.any(ModelVersionChange.class));
    Mockito.verify(command).printInformation(Mockito.contains("version 99 aliases changed."));
  }

  @Test
  void validateBothAliasAndVersionNotNullShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionAliases command =
        new UpdateModelVersionAliases(
            context,
            "metalake1",
            "catalog1",
            "schema1",
            "model1",
            1,
            "alias1",
            new String[] {"aliasA"},
            new String[] {"aliasB"});

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::validate);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void validateBothAliasAndVersionNullShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionAliases command =
        new UpdateModelVersionAliases(
            context,
            "metalake1",
            "catalog1",
            "schema1",
            "model1",
            null,
            null,
            new String[] {"aliasA"},
            new String[] {"aliasB"});

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::validate);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void validateOnlyAliasSetShouldPass() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionAliases command =
        new UpdateModelVersionAliases(
            context,
            "metalake1",
            "catalog1",
            "schema1",
            "model1",
            null,
            "alias1",
            new String[] {"aliasA"},
            new String[] {"aliasB"});

    Assertions.assertDoesNotThrow(command::validate);
  }

  @Test
  void validateOnlyVersionSetShouldPass() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionAliases command =
        new UpdateModelVersionAliases(
            context,
            "metalake1",
            "catalog1",
            "schema1",
            "model1",
            1,
            null,
            new String[] {"aliasA"},
            new String[] {"aliasB"});

    Assertions.assertDoesNotThrow(command::validate);
  }
}
