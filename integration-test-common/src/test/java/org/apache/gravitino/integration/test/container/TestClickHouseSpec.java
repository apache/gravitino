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
 */
package org.apache.gravitino.integration.test.container;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

/** Tests {@link ClickHouseSpec} and {@link ClickHouseFixture} without Docker. */
public class TestClickHouseSpec {

  private static final TestDatabaseName DATABASE =
      TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT;

  @Test
  void testSingleNodeFixtureMapsToTheSpec() {
    Assertions.assertEquals(
        List.of(new ClickHouseSpec(1, DATABASE)),
        ContainerExtension.specs(SingleNodeFixture.class));
  }

  @Test
  void testClusterFixtureMapsToTheSpec() {
    Assertions.assertEquals(
        List.of(new ClickHouseSpec(3, TestDatabaseName.CLICKHOUSE_CLUSTER_CLICKHOUSE_IT)),
        ContainerExtension.specs(ClusterFixture.class));
  }

  @Test
  void testContainerType() {
    Assertions.assertEquals(
        ClickHouseContainer.class, new ClickHouseSpec(1, DATABASE).containerType());
  }

  @Test
  void testSingleNodeStartsWithTheDatabase() {
    ContainerSuite suite = Mockito.mock(ContainerSuite.class);
    ClickHouseContainer clickHouse = Mockito.mock(ClickHouseContainer.class);
    Mockito.when(suite.getClickHouseContainer()).thenReturn(clickHouse);

    Assertions.assertSame(clickHouse, new ClickHouseSpec(1, DATABASE).getOrStart(suite));

    InOrder order = Mockito.inOrder(suite);
    order.verify(suite).startClickHouseContainer(DATABASE);
    order.verify(suite).getClickHouseContainer();
    Mockito.verify(suite, Mockito.never())
        .startClickHouseClusterContainer(Mockito.any(), Mockito.anyString());
  }

  @Test
  void testClusterStartsWithTheDatabaseAndTheTemplate() throws Exception {
    ContainerSuite suite = Mockito.mock(ContainerSuite.class);
    ClickHouseContainer firstNode = Mockito.mock(ClickHouseContainer.class);
    Mockito.when(suite.getClickHouseClusterContainer()).thenReturn(firstNode);

    Assertions.assertSame(firstNode, new ClickHouseSpec(3, DATABASE).getOrStart(suite));

    ArgumentCaptor<String> templatePath = ArgumentCaptor.forClass(String.class);
    InOrder order = Mockito.inOrder(suite);
    order
        .verify(suite)
        .startClickHouseClusterContainer(Mockito.eq(DATABASE), templatePath.capture());
    order.verify(suite).getClickHouseClusterContainer();
    Mockito.verify(suite, Mockito.never()).startClickHouseContainer(Mockito.any());

    Path template = Paths.get(templatePath.getValue());
    Assertions.assertTrue(Files.isReadable(template));
    Assertions.assertTrue(
        Files.readString(template, StandardCharsets.UTF_8).contains("${ZOOKEEPER_HOST}"));
  }

  @Test
  void testClusterTemplateIsCopiedOnce() {
    ContainerSuite suite = Mockito.mock(ContainerSuite.class);
    ClickHouseSpec spec = new ClickHouseSpec(3, DATABASE);

    spec.getOrStart(suite);
    spec.getOrStart(suite);

    ArgumentCaptor<String> templatePath = ArgumentCaptor.forClass(String.class);
    Mockito.verify(suite, Mockito.times(2))
        .startClickHouseClusterContainer(Mockito.eq(DATABASE), templatePath.capture());
    Assertions.assertEquals(templatePath.getAllValues().get(0), templatePath.getAllValues().get(1));
  }

  @Test
  void testOtherNodeCountsAreRejected() {
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new ClickHouseSpec(2, DATABASE));
    Assertions.assertTrue(e.getMessage().contains("1 or 3"), e.getMessage());
  }

  @Test
  void testFixtureWithAnUnsupportedNodeCountIsRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ContainerExtension.specs(TwoNodeFixture.class));
  }

  // The build excludes this tag, so Gradle never runs the fixture classes on their own.
  @Tag("gravitino-test-fixture")
  @ClickHouseFixture(database = TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT)
  static class SingleNodeFixture {}

  @Tag("gravitino-test-fixture")
  @ClickHouseFixture(nodes = 3, database = TestDatabaseName.CLICKHOUSE_CLUSTER_CLICKHOUSE_IT)
  static class ClusterFixture {}

  @Tag("gravitino-test-fixture")
  @ClickHouseFixture(nodes = 2, database = TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT)
  static class TwoNodeFixture {}
}
