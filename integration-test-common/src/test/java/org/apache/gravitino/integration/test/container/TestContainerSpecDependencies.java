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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestContainerSpecDependencies {

  private static final List<String> STARTED = new ArrayList<>();
  // Each node's direct dependencies; the graph a test sets before it starts a node.
  private static Map<String, List<String>> graph = Map.of();

  @BeforeEach
  void clear() {
    STARTED.clear();
  }

  @Test
  void testDependenciesStartDeepestFirst() {
    graph = Map.of("hive", List.of("ranger", "localstack"), "ranger", List.of("solr"));

    ContainerSpec.startWithDependencies(new Node("hive"), null);

    Assertions.assertEquals(List.of("solr", "ranger", "localstack", "hive"), STARTED);
  }

  @Test
  void testPrivateStartOnlyStartsDependencies() {
    graph = Map.of("hive", List.of("ranger"));

    ContainerSpec.startDependencies(new Node("hive"), null);

    Assertions.assertEquals(List.of("ranger"), STARTED);
  }

  @Test
  void testCycleFailsClearly() {
    graph = Map.of("a", List.of("b"), "b", List.of("a"));

    IllegalStateException e =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> ContainerSpec.startWithDependencies(new Node("a"), null));

    Assertions.assertTrue(e.getMessage().contains("cycle"), e.getMessage());
    Assertions.assertTrue(STARTED.isEmpty());
  }

  @Test
  void testHiveSpecDeclaresWhatItNeeds() {
    Assertions.assertEquals(
        List.of(new RangerSpec()), new HiveSpec(HiveVersion.HIVE3, HiveAuth.RANGER).dependencies());
    Assertions.assertEquals(
        List.of(new LocalStackSpec()),
        new HiveSpec(HiveVersion.HIVE3, HiveAuth.NONE, HiveStorage.S3).dependencies());
    Assertions.assertEquals(
        List.of(), new HiveSpec(HiveVersion.HIVE2, HiveAuth.NONE).dependencies());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new HiveSpec(HiveVersion.HIVE2, HiveAuth.RANGER));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new HiveSpec(HiveVersion.HIVE3, HiveAuth.KERBEROS, HiveStorage.S3));
  }

  @Test
  void testRangerHiveStartsLazily() {
    HiveSpec.Factory factory = new HiveSpec.Factory();

    Assertions.assertEquals(
        Startup.LAZY, factory.startupFor(RangerHiveOnly.class.getAnnotation(HiveFixture.class)));
    Assertions.assertEquals(
        Startup.EAGER, factory.startupFor(PlainHiveOnly.class.getAnnotation(HiveFixture.class)));
  }

  @HiveFixture(version = HiveVersion.HIVE3, auth = HiveAuth.RANGER)
  static class RangerHiveOnly {}

  @HiveFixture
  static class PlainHiveOnly {}

  // Only ever mocked, so the constructor never runs.
  abstract static class FakeContainer extends BaseContainer {
    FakeContainer() {
      super((String) null, null, null, null, null, null, Optional.empty());
    }
  }

  record Node(String name) implements ContainerSpec<FakeContainer> {
    @Override
    public Class<FakeContainer> containerType() {
      return FakeContainer.class;
    }

    @Override
    public List<ContainerSpec<?>> dependencies() {
      return graph.getOrDefault(name, List.of()).stream().<ContainerSpec<?>>map(Node::new).toList();
    }

    @Override
    public FakeContainer getOrStart(ContainerSuite suite) {
      STARTED.add(name);
      return Mockito.mock(FakeContainer.class);
    }
  }
}
