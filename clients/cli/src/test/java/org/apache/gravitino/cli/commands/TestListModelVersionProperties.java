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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestListModelVersionProperties {

  @BeforeEach
  void setUp() {
    Main.useExit = false;
  }

  @AfterEach
  void tearDown() {
    Main.useExit = true;
  }

  @Test
  void handleByVersionPrintsProperties() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    ListModelVersionProperties command =
        Mockito.spy(
            new ListModelVersionProperties(
                context, "metalake1", "catalog1", "schema1", "model1", 1, null));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);
    ModelVersion mockModelVersion = Mockito.mock(ModelVersion.class);

    Map<String, String> properties = new HashMap<>();
    properties.put("vp1", "vval1");
    properties.put("vp2", "vval2");

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);
    Mockito.when(mockModelCatalog.getModelVersion(Mockito.any(NameIdentifier.class), Mockito.eq(1)))
        .thenReturn(mockModelVersion);
    Mockito.when(mockModelVersion.properties()).thenReturn(properties);

    command.handle();

    Mockito.verify(mockModelCatalog).getModelVersion(NameIdentifier.of("schema1", "model1"), 1);
    Mockito.verify(command).printProperties(properties);
  }

  @Test
  void handleByAliasPrintsProperties() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    ListModelVersionProperties command =
        Mockito.spy(
            new ListModelVersionProperties(
                context, "metalake1", "catalog1", "schema1", "model1", null, "v1"));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);
    ModelVersion mockModelVersion = Mockito.mock(ModelVersion.class);

    Map<String, String> properties = new HashMap<>();
    properties.put("vp1", "vval1");

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);
    Mockito.when(
            mockModelCatalog.getModelVersion(Mockito.any(NameIdentifier.class), Mockito.eq("v1")))
        .thenReturn(mockModelVersion);
    Mockito.when(mockModelVersion.properties()).thenReturn(properties);

    command.handle();

    Mockito.verify(mockModelCatalog).getModelVersion(NameIdentifier.of("schema1", "model1"), "v1");
    Mockito.verify(command).printProperties(properties);
  }

  @Test
  void handleWithBothNullVersionAndAliasShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    ListModelVersionProperties command =
        new ListModelVersionProperties(
            context, "metalake1", "catalog1", "schema1", "model1", null, null);

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::handle);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void validateWithBothVersionAndAliasShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    ListModelVersionProperties command =
        new ListModelVersionProperties(
            context, "metalake1", "catalog1", "schema1", "model1", 1, "alias1");

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::validate);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void handleWithEmptyProperties() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    ListModelVersionProperties command =
        Mockito.spy(
            new ListModelVersionProperties(
                context, "metalake1", "catalog1", "schema1", "model1", 0, null));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);
    ModelVersion mockModelVersion = Mockito.mock(ModelVersion.class);

    Map<String, String> properties = new HashMap<>();

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);
    Mockito.when(mockModelCatalog.getModelVersion(Mockito.any(NameIdentifier.class), Mockito.eq(0)))
        .thenReturn(mockModelVersion);
    Mockito.when(mockModelVersion.properties()).thenReturn(properties);

    command.handle();

    Mockito.verify(command).printProperties(properties);
  }
}
