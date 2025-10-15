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

class TestUpdateModelVersionComment {

  @BeforeEach
  void setUp() {
    Main.useExit = false;
  }

  @AfterEach
  void tearDown() {
    Main.useExit = true;
  }

  @Test
  void handleWithAliasCallsAlterModelVersionWithAlias() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionComment command =
        Mockito.spy(
            new UpdateModelVersionComment(
                context,
                "metalake1",
                "catalog1",
                "schema1",
                "model1",
                null,
                "alias1",
                "new comment"));

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
            Mockito.eq("alias1"),
            ArgumentMatchers.any(ModelVersionChange.class));
    Mockito.verify(command).printInformation(Mockito.contains("alias alias1 comment changed."));
  }

  @Test
  void handleWithVersionCallsAlterModelVersionWithVersion() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionComment command =
        Mockito.spy(
            new UpdateModelVersionComment(
                context, "metalake1", "catalog1", "schema1", "model1", 2, null, "new comment"));

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
            Mockito.eq(2),
            ArgumentMatchers.any(ModelVersionChange.class));
    Mockito.verify(command).printInformation(Mockito.contains("version 2 comment changed."));
  }

  @Test
  void validateBothAliasAndVersionNotNullShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionComment command =
        new UpdateModelVersionComment(
            context, "metalake1", "catalog1", "schema1", "model1", 1, "alias1", "comment");

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::validate);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void validateBothAliasAndVersionNullShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionComment command =
        new UpdateModelVersionComment(
            context, "metalake1", "catalog1", "schema1", "model1", null, null, "comment");

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::validate);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void validateOnlyAliasSetShouldPass() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionComment command =
        new UpdateModelVersionComment(
            context, "metalake1", "catalog1", "schema1", "model1", null, "alias1", "comment");

    Assertions.assertDoesNotThrow(command::validate);
  }

  @Test
  void validateOnlyVersionSetShouldPass() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionComment command =
        new UpdateModelVersionComment(
            context, "metalake1", "catalog1", "schema1", "model1", 1, null, "comment");

    Assertions.assertDoesNotThrow(command::validate);
  }
}
