/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */ package org.apache.gravitino.integration.test.container;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTrinoSpec {

  @Test
  void testFixtureSpecHasNoPortUntilBoundToAServerTest() {
    TrinoSpec unbound = (TrinoSpec) ContainerExtension.specs(TrinoOnly.class).get(0);

    Assertions.assertEquals(new TrinoSpec(478, 2, HiveVersion.HIVE3, -1), unbound);
    Assertions.assertEquals(
        new TrinoSpec(478, 2, HiveVersion.HIVE3, 8090),
        unbound.forTestInstance((GravitinoServerAware) () -> 8090));
    Assertions.assertSame(unbound, unbound.forTestInstance(new Object()));
  }

  @Test
  void testUnboundSpecRefusesToStart() {
    IllegalStateException e =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> new TrinoSpec(478, 0, HiveVersion.HIVE2, -1).getOrStart(null));

    Assertions.assertTrue(e.getMessage().contains("GravitinoServerAware"), e.getMessage());
  }

  @Test
  void testTrinoFixtureStartsLazilyByDefault() throws NoSuchMethodException {
    Assertions.assertEquals(
        Startup.LAZY, TrinoFixture.class.getMethod("startup").getDefaultValue());
    Assertions.assertEquals(List.of(), new TrinoSpec(478, 0, HiveVersion.HIVE2, 1).dependencies());
  }

  @Test
  void testPrivateClusterNeedsAServerToo() {
    TrinoSpec unbound = new TrinoSpec(478, 1, HiveVersion.HIVE2, -1);

    Assertions.assertTrue(unbound.supportsPrivate());
    IllegalStateException e =
        Assertions.assertThrows(
            IllegalStateException.class, () -> unbound.startPrivate(null, Durability.DISPOSABLE));
    Assertions.assertTrue(e.getMessage().contains("GravitinoServerAware"), e.getMessage());
  }

  @Test
  void testNodeDependsOnTheSharedHive2AndNeedsAServer() {
    TrinoNodeSpec node = new TrinoNodeSpec("lake", 8090);

    Assertions.assertEquals(
        List.of(new HiveSpec(HiveVersion.HIVE2, HiveAuth.NONE)), node.dependencies());
    Assertions.assertEquals(TrinoContainer.class, node.containerType());
    Assertions.assertTrue(node.supportsPrivate());
    Assertions.assertThrows(IllegalArgumentException.class, () -> new TrinoNodeSpec("lake", -1));
  }

  @Test
  void testNodeIsBuiltForItsServerMetalakeAndHive() {
    TrinoContainer node =
        new TrinoNodeSpec("lake", 8090).builder("10.0.0.5", "/connector", null).build();

    Assertions.assertEquals(TestImages.TRINO, node.getContainer().getDockerImageName());
    Assertions.assertEquals("8090", node.getContainer().getEnvMap().get("GRAVITINO_HOST_PORT"));
    Assertions.assertEquals("lake", node.getContainer().getEnvMap().get("GRAVITINO_METALAKE_NAME"));
    Assertions.assertTrue(
        node.getContainer().getExtraHosts().contains(HiveContainer.HOST_NAME + ":10.0.0.5"),
        node.getContainer().getExtraHosts().toString());
    Assertions.assertEquals(
        List.of(TrinoContainer.TRINO_PORT), node.getContainer().getExposedPorts());
  }

  @Test
  void testNodePluginFilesAddEveryExtraJar(@TempDir Path extra) throws IOException {
    Files.createFile(extra.resolve("a.jar"));
    Files.createFile(extra.resolve("notes.txt"));
    String plugin = TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR;

    Assertions.assertEquals(
        Map.of(plugin, "/connector"), TrinoNodeSpec.pluginFiles("/connector", null));
    Assertions.assertEquals(
        Map.of(plugin, "/connector", plugin + "/a.jar", extra.resolve("a.jar").toString()),
        TrinoNodeSpec.pluginFiles("/connector", extra.toString()));
  }

  @TrinoFixture(workers = 2, hive = HiveVersion.HIVE3)
  static class TrinoOnly {}
}
