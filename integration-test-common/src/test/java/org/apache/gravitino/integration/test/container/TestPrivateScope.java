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

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests, without Docker, that every fixture can ask for a private container. */
public class TestPrivateScope {

  private static final int FIXTURE_COUNT = 14;

  @Test
  void testEveryFixtureReadsItsScope() throws Exception {
    List<ContainerFixture.SpecFactory<Annotation>> factories = new ArrayList<>();
    for (Annotation fixture : fixtures(MethodScopedFixtures.class)) {
      Assertions.assertEquals(Scope.METHOD, factory(fixture).scopeFor(fixture), fixture.toString());
    }
    for (Annotation fixture : fixtures(DefaultFixtures.class)) {
      Assertions.assertEquals(Scope.RUN, factory(fixture).scopeFor(fixture), fixture.toString());
      factories.add(factory(fixture));
    }
    Assertions.assertEquals(FIXTURE_COUNT, factories.size());
  }

  @Test
  void testEverySpecButTrinoAndTheClickHouseClusterSupportsPrivate() throws Exception {
    List<String> withoutPrivate = new ArrayList<>();
    for (Annotation fixture : fixtures(DefaultFixtures.class)) {
      ContainerSpec<?> spec = factory(fixture).specFor(fixture);
      if (!spec.supportsPrivate()) {
        withoutPrivate.add(spec.getClass().getSimpleName());
      }
    }
    Assertions.assertEquals(List.of("TrinoSpec"), withoutPrivate);
    Assertions.assertFalse(
        new ClickHouseSpec(3, TestDatabaseName.CLICKHOUSE_CLUSTER_CLICKHOUSE_IT).supportsPrivate());
    for (HiveStorage storage : HiveStorage.values()) {
      Assertions.assertTrue(
          new HiveSpec(HiveVersion.HIVE3, HiveAuth.NONE, storage).supportsPrivate());
    }
    Assertions.assertTrue(new HiveSpec(HiveVersion.HIVE3, HiveAuth.RANGER).supportsPrivate());
  }

  @Test
  void testSpecsWithoutPrivateContainersSayWhy() {
    ClickHouseSpec cluster =
        new ClickHouseSpec(3, TestDatabaseName.CLICKHOUSE_CLUSTER_CLICKHOUSE_IT);
    UnsupportedOperationException e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> cluster.startPrivate(Mockito.mock(SharedContainers.class), Durability.DURABLE));

    Assertions.assertEquals("ClickHouseSpec " + cluster.noPrivateReason(), e.getMessage());
    Assertions.assertTrue(cluster.noPrivateReason().contains("fixed host names"));
  }

  static Stream<Arguments> privateSpecs() {
    return Stream.of(
        Arguments.of(new KafkaSpec(), KafkaContainer.Builder.class, "gravitino-ci-kafka"),
        Arguments.of(
            new ZooKeeperSpec(), ZooKeeperContainer.Builder.class, ZooKeeperContainer.HOST_NAME),
        Arguments.of(
            new LocalStackSpec(),
            GravitinoLocalStackContainer.Builder.class,
            GravitinoLocalStackContainer.HOST_NAME),
        Arguments.of(
            new MotoSpec(), GravitinoMotoContainer.Builder.class, GravitinoMotoContainer.HOST_NAME),
        Arguments.of(new RustFSSpec(), RustFSContainer.Builder.class, RustFSContainer.HOST_NAME),
        Arguments.of(
            new StarRocksSpec(), StarRocksContainer.Builder.class, StarRocksContainer.HOST_NAME),
        Arguments.of(
            new OceanBaseSpec(), OceanBaseContainer.Builder.class, OceanBaseContainer.HOST_NAME),
        Arguments.of(new RangerSpec(), RangerContainer.Builder.class, RangerContainer.HOST_NAME),
        Arguments.of(
            new DorisSpec(DorisImageName.VERSION_2_1),
            DorisContainer.Builder.class,
            "gravitino-ci-doris-version21"),
        Arguments.of(
            new HiveSpec(HiveVersion.HIVE3, HiveAuth.NONE),
            HiveContainer.Builder.class,
            HiveContainer.HOST_NAME),
        Arguments.of(
            new HiveSpec(HiveVersion.HIVE2, HiveAuth.KERBEROS),
            HiveContainer.Builder.class,
            "gravitino-ci-kerberos-hive"));
  }

  @ParameterizedTest
  @MethodSource("privateSpecs")
  void testPrivateStartBuildsLikeTheSharedContainerAndLeavesItAlone(
      ContainerSpec<?> spec, Class<?> builderType, String hostName) {
    SharedContainers shared = Mockito.mock(SharedContainers.class);

    spec.startPrivate(shared, Durability.DURABLE);

    BaseContainer.Builder<?, ?> builder = verifyPrivateStart(shared, false).builder();
    Assertions.assertInstanceOf(builderType, builder);
    Assertions.assertEquals(hostName, builder.hostName);
    // No shared start* or get* call: the shared container is neither started nor replaced.
    Mockito.verifyNoMoreInteractions(shared);
  }

  static Stream<Arguments> databaseSpecs() {
    return Stream.of(
        Arguments.of(
            new MySQLSpec(MySQLVersion.MYSQL_5_7, TestDatabaseName.MYSQL_CATALOG_MYSQL_IT),
            MySQLContainer.class,
            "gravitino-ci-mysql-v5"),
        Arguments.of(
            new MySQLSpec(MySQLVersion.MYSQL_8_0, TestDatabaseName.MYSQL_CATALOG_MYSQL_IT),
            MySQLContainer.class,
            "gravitino-ci-mysql"),
        Arguments.of(
            new PostgreSQLSpec(PGImageName.VERSION_13, TestDatabaseName.PG_CATALOG_POSTGRESQL_IT),
            PostgreSQLContainer.class,
            PostgreSQLContainer.HOST_NAME),
        Arguments.of(
            new ClickHouseSpec(1, TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT),
            ClickHouseContainer.class,
            ClickHouseContainer.HOST_NAME));
  }

  @ParameterizedTest
  @MethodSource("databaseSpecs")
  void testPrivateDatabaseContainerCreatesTheFixtureDatabase(
      ContainerSpec<?> spec, Class<? extends BaseContainer> containerType, String hostName)
      throws Exception {
    SharedContainers shared = Mockito.mock(SharedContainers.class);

    spec.startPrivate(shared, Durability.DISPOSABLE);

    PrivateStart start = verifyPrivateStart(shared, true);
    Assertions.assertEquals(hostName, start.builder().hostName);
    Mockito.verifyNoMoreInteractions(shared);
    BaseContainer container = Mockito.mock(containerType);
    start.setUp().accept(container);
    TestDatabaseName database =
        (TestDatabaseName) spec.getClass().getMethod("database").invoke(spec);
    containerType
        .getMethod("createDatabase", TestDatabaseName.class)
        .invoke(Mockito.verify(container), database);
  }

  @Test
  void testPrivateRangerHiveReportsToTheSharedRanger() {
    SharedContainers shared = Mockito.mock(SharedContainers.class);
    RangerContainer ranger = Mockito.mock(RangerContainer.class);
    Mockito.when(ranger.getContainerIpAddress()).thenReturn("10.20.30.5");
    Mockito.when(shared.getRangerContainer()).thenReturn(ranger);

    new HiveSpec(HiveVersion.HIVE3, HiveAuth.RANGER).startPrivate(shared, Durability.DURABLE);

    BaseContainer.Builder<?, ?> builder = verifyPrivateStart(shared, false).builder();
    Assertions.assertEquals("gravitino-ci-hive-ranger", builder.hostName);
    Assertions.assertEquals(
        "http://10.20.30.5:" + RangerContainer.RANGER_SERVER_PORT,
        builder.envVars.get(RangerContainer.DOCKER_ENV_RANGER_SERVER_URL));
    // The shared Ranger is a dependency; the shared Ranger Hive is never started.
    Mockito.verify(shared, Mockito.never()).startHiveRangerContainer(Mockito.any());
  }

  @Test
  void testPrivateS3HiveKeepsItsTablesInTheSharedLocalStack() {
    SharedContainers shared = Mockito.mock(SharedContainers.class);
    GravitinoLocalStackContainer localStack = Mockito.mock(GravitinoLocalStackContainer.class);
    Mockito.when(localStack.getContainerIpAddress()).thenReturn("10.20.30.6");
    Mockito.when(shared.getLocalStackContainer()).thenReturn(localStack);

    new HiveSpec(HiveVersion.HIVE3, HiveAuth.NONE, HiveStorage.S3)
        .startPrivate(shared, Durability.DURABLE);

    BaseContainer.Builder<?, ?> builder = verifyPrivateStart(shared, false).builder();
    Assertions.assertEquals(
        "http://10.20.30.6:" + GravitinoLocalStackContainer.PORT,
        builder.envVars.get("S3_ENDPOINT"));
    Mockito.verify(shared, Mockito.never())
        .startStorageHiveContainer(Mockito.any(), Mockito.any(), Mockito.any());
  }

  /** What a spec passed to {@link SharedContainers#startPrivate}. */
  record PrivateStart(BaseContainer.Builder<?, ?> builder, Consumer<BaseContainer> setUp) {}

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PrivateStart verifyPrivateStart(SharedContainers shared, boolean withSetUp) {
    ArgumentCaptor<BaseContainer.Builder> builder =
        ArgumentCaptor.forClass(BaseContainer.Builder.class);
    if (!withSetUp) {
      Mockito.verify(shared).startPrivate(builder.capture(), Mockito.eq(Durability.DURABLE));
      return new PrivateStart(builder.getValue(), container -> {});
    }
    ArgumentCaptor<Consumer> setUp = ArgumentCaptor.forClass(Consumer.class);
    Mockito.verify(shared)
        .startPrivate(builder.capture(), Mockito.eq(Durability.DISPOSABLE), setUp.capture());
    return new PrivateStart(builder.getValue(), setUp.getValue());
  }

  private static List<Annotation> fixtures(Class<?> testClass) {
    return Stream.of(testClass.getDeclaredAnnotations())
        .filter(a -> a.annotationType().isAnnotationPresent(ContainerFixture.class))
        .toList();
  }

  @SuppressWarnings("unchecked")
  private static ContainerFixture.SpecFactory<Annotation> factory(Annotation fixture)
      throws Exception {
    Constructor<? extends ContainerFixture.SpecFactory<?>> constructor =
        fixture
            .annotationType()
            .getAnnotation(ContainerFixture.class)
            .value()
            .getDeclaredConstructor();
    constructor.setAccessible(true);
    return (ContainerFixture.SpecFactory<Annotation>) constructor.newInstance();
  }

  // The build excludes this tag; these classes only carry annotations.
  @Tag("gravitino-test-fixture")
  @ClickHouseFixture(database = TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT)
  @DorisFixture
  @HiveFixture
  @KafkaFixture
  @LocalStackFixture
  @MotoFixture
  @MySQLFixture(database = TestDatabaseName.MYSQL_CATALOG_MYSQL_IT)
  @OceanBaseFixture
  @PostgreSQLFixture(database = TestDatabaseName.PG_CATALOG_POSTGRESQL_IT)
  @RangerFixture
  @RustFSFixture
  @StarRocksFixture
  @TrinoFixture
  @ZooKeeperFixture
  static class DefaultFixtures {}

  @Tag("gravitino-test-fixture")
  @ClickHouseFixture(
      database = TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT,
      scope = Scope.METHOD)
  @DorisFixture(scope = Scope.METHOD)
  @HiveFixture(scope = Scope.METHOD)
  @KafkaFixture(scope = Scope.METHOD)
  @LocalStackFixture(scope = Scope.METHOD)
  @MotoFixture(scope = Scope.METHOD)
  @MySQLFixture(database = TestDatabaseName.MYSQL_CATALOG_MYSQL_IT, scope = Scope.METHOD)
  @OceanBaseFixture(scope = Scope.METHOD)
  @PostgreSQLFixture(database = TestDatabaseName.PG_CATALOG_POSTGRESQL_IT, scope = Scope.METHOD)
  @RangerFixture(scope = Scope.METHOD)
  @RustFSFixture(scope = Scope.METHOD)
  @StarRocksFixture(scope = Scope.METHOD)
  @TrinoFixture(scope = Scope.METHOD)
  @ZooKeeperFixture(scope = Scope.METHOD)
  static class MethodScopedFixtures {}
}
