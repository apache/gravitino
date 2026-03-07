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
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestListModelProperties {

  @BeforeEach
  void setUp() {
    Main.useExit = false;
  }

  @AfterEach
  void tearDown() {
    Main.useExit = true;
  }

  @Test
  void handlePrintsModelProperties() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    ListModelProperties command =
        Mockito.spy(new ListModelProperties(context, "metalake1", "catalog1", "schema1", "model1"));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);
    Model mockModel = Mockito.mock(Model.class);

    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);
    Mockito.when(mockModelCatalog.getModel(Mockito.any(NameIdentifier.class)))
        .thenReturn(mockModel);
    Mockito.when(mockModel.properties()).thenReturn(properties);

    command.handle();

    Mockito.verify(mockModelCatalog).getModel(NameIdentifier.of("schema1", "model1"));
    Mockito.verify(command).printProperties(properties);
  }

  @Test
  void handleWithEmptyProperties() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    ListModelProperties command =
        Mockito.spy(new ListModelProperties(context, "metalake1", "catalog1", "schema1", "model1"));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);
    Model mockModel = Mockito.mock(Model.class);

    Map<String, String> properties = new HashMap<>();

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);
    Mockito.when(mockModelCatalog.getModel(Mockito.any(NameIdentifier.class)))
        .thenReturn(mockModel);
    Mockito.when(mockModel.properties()).thenReturn(properties);

    command.handle();

    Mockito.verify(command).printProperties(properties);
  }
}
