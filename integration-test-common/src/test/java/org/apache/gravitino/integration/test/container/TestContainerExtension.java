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

import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.EventConditions.test;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.testkit.engine.EngineExecutionResults;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.mockito.Mockito;

/** Runs small fixture classes through a real JUnit engine, with a fake spec instead of Docker. */
public class TestContainerExtension {

  // Gradle would otherwise discover and run these nested fixtures directly;
  // integration-test-common/build.gradle.kts excludes the tag. Keep the two in sync.
  private static final String FIXTURE = "gravitino-test-fixture";

  private static final List<String> LOG = new ArrayList<>();
  private static final FakeContainer FIRST = Mockito.mock(FakeContainer.class);
  private static final FakeContainer SECOND = Mockito.mock(FakeContainer.class);
  private static final OtherContainer OTHER = Mockito.mock(OtherContainer.class);
  // Every private container FakeSpec.startPrivate returned, in start order.
  private static final List<FakeContainer> PRIVATE = new ArrayList<>();

  @BeforeEach
  void clearLog() {
    LOG.clear();
    PRIVATE.clear();
  }

  @Test
  void testFixtureStartsBeforeAllAndInjectsByType() {
    run(InjectedFixture.class).testEvents().assertStatistics(stats -> stats.succeeded(2).failed(0));

    // The callback starts the container before @BeforeAll; each parameter then gets the running
    // one.
    Assertions.assertEquals(
        List.of(
            "start first",
            "start first",
            "beforeAll",
            "start first",
            "test",
            "start first",
            "test"),
        LOG);
  }

  @Test
  void testFieldIsFilledBeforeAnOverriddenBeforeAll() {
    run(FieldFixture.class).testEvents().assertStatistics(stats -> stats.succeeded(1).failed(0));

    Assertions.assertEquals(List.of("start first", "start first", "setUp"), LOG);
  }

  @Test
  void testStaticFieldIsFilledBeforeAStaticBeforeAll() {
    run(StaticFieldFixture.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(1).failed(0));
  }

  @Test
  void testFieldFixturesGiveOneClassTwoContainersOfOneType() {
    run(TwoFieldFixtures.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(1).failed(0));
    run(TwoStaticFieldFixtures.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(1).failed(0));
    run(TwoFieldFixturesPerMethod.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(1).failed(0));
  }

  @Test
  void testFieldFixtureOfWrongTypeFailsClearly() {
    run(WrongTypeFieldFixture.class)
        .allEvents()
        .assertThatEvents()
        .haveExactly(
            1,
            event(
                finishedWithFailure(
                    instanceOf(ExtensionConfigurationException.class),
                    message(m -> m.contains("cannot hold a OtherContainer")))));
  }

  @Test
  void testLazyFixtureStartsWhenAsked() {
    run(LazyFixture.class).testEvents().assertStatistics(stats -> stats.succeeded(1).failed(0));

    // The superclass @BeforeAll runs before the container starts, as BaseIT's server start would.
    Assertions.assertEquals(List.of("setUp", "start first", "test"), LOG);
  }

  @Test
  void testUnusedFixtureWarns() {
    List<String> warnings = new ArrayList<>();
    Consumer<String> original = ContainerExtension.warnings;
    ContainerExtension.warnings = warnings::add;
    try {
      run(UnusedFixture.class).testEvents().assertStatistics(stats -> stats.succeeded(1).failed(0));
      run(InjectedFixture.class).testEvents().assertStatistics(stats -> stats.succeeded(2));
      run(FieldFixture.class).testEvents().assertStatistics(stats -> stats.succeeded(1));
    } finally {
      ContainerExtension.warnings = original;
    }

    Assertions.assertEquals(1, warnings.size(), warnings.toString());
    Assertions.assertTrue(
        warnings.get(0).startsWith("UnusedFixture declares a fixture for FakeSpec"));
  }

  @Test
  void testClickHouseClusterOnlySuppliedForThreeNodes() {
    ClickHouseSpec single =
        new ClickHouseSpec(1, TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT);
    ClickHouseSpec cluster =
        new ClickHouseSpec(3, TestDatabaseName.CLICKHOUSE_CLUSTER_CLICKHOUSE_IT);

    Assertions.assertTrue(single.supplies(ClickHouseContainer.class));
    Assertions.assertFalse(single.supplies(ClickHouseCluster.class));
    Assertions.assertTrue(cluster.supplies(ClickHouseContainer.class));
    Assertions.assertTrue(cluster.supplies(ClickHouseCluster.class));
    Assertions.assertTrue(cluster.supplies(BaseContainer.class));
  }

  @Test
  void testFinalFieldFailsClearly() {
    run(FinalFieldFixture.class)
        .testEvents()
        .assertThatEvents()
        .haveExactly(
            1,
            event(
                test("test"),
                finishedWithFailure(
                    instanceOf(ExtensionConfigurationException.class),
                    message(m -> m.contains("must not be final")))));
  }

  @Test
  void testSuperclassFixtureApplies() {
    run(SubclassFixture.class).testEvents().assertStatistics(stats -> stats.succeeded(1).failed(0));
  }

  @Test
  void testSubclassFixtureReplacesSuperclassFixtureOfSameType() {
    run(OverridingFixture.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(1).failed(0));

    Assertions.assertEquals(
        List.of(new FakeSpec("second")), ContainerExtension.specs(OverridingFixture.class));
    Assertions.assertFalse(LOG.contains("start first"), LOG.toString());
  }

  @Test
  void testTwoFixturesOfOneTypeFailClearly() {
    run(AmbiguousFixture.class)
        .testEvents()
        .assertThatEvents()
        .haveExactly(
            1,
            event(
                test("test"), finishedWithFailure(instanceOf(ParameterResolutionException.class))));
  }

  @Test
  void testSpecsReadsEveryFixture() {
    Assertions.assertEquals(
        List.of(new FakeSpec("first"), new FakeSpec("second")),
        ContainerExtension.specs(AmbiguousFixture.class));
    Assertions.assertEquals(
        List.of(new KafkaSpec()), ContainerExtension.specs(KafkaFixtureOnly.class));
    Assertions.assertEquals(
        List.of(new HiveSpec(HiveVersion.HIVE3, HiveAuth.KERBEROS)),
        ContainerExtension.specs(HiveFixtureOnly.class));
    Assertions.assertEquals(
        List.of(
            new MySQLSpec(MySQLVersion.MYSQL_5_7, TestDatabaseName.MYSQL_CATALOG_MYSQL_IT),
            new PostgreSQLSpec(PGImageName.VERSION_13, TestDatabaseName.PG_CATALOG_POSTGRESQL_IT)),
        ContainerExtension.specs(DatabaseFixturesOnly.class));
  }

  @Test
  void testClassScopeStartsOnePrivateContainerForTheClass() {
    run(ClassScopeFixture.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(2).failed(0));

    Assertions.assertEquals(List.of("startPrivate first"), LOG);
    Assertions.assertEquals(1, PRIVATE.size());
    Mockito.verify(PRIVATE.get(0), Mockito.times(1)).close();
  }

  @Test
  void testMethodScopeStartsOnePrivateContainerPerTest() {
    run(MethodScopeFixture.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(2).failed(0));

    Assertions.assertEquals(
        List.of("startPrivate first", "test", "startPrivate first", "test"), LOG);
    Assertions.assertEquals(2, PRIVATE.size());
    Assertions.assertNotSame(PRIVATE.get(0), PRIVATE.get(1));
    for (FakeContainer container : PRIVATE) {
      Mockito.verify(container, Mockito.times(1)).close();
    }
  }

  @Test
  void testRunAndClassScopeFixturesResolveToDifferentContainers() {
    run(MixedScopeFixture.class)
        .testEvents()
        .assertStatistics(stats -> stats.succeeded(1).failed(0));

    Assertions.assertEquals(1, Collections.frequency(LOG, "startPrivate first"), LOG.toString());
    Assertions.assertFalse(LOG.contains("start first"), LOG.toString());
  }

  @Test
  void testMethodScopeStaticFieldFailsClearly() {
    run(MethodScopeStaticFieldFixture.class)
        .allEvents()
        .assertThatEvents()
        .haveExactly(
            1,
            event(
                finishedWithFailure(
                    instanceOf(ExtensionConfigurationException.class),
                    message(m -> m.contains("Scope.METHOD")))));
    Assertions.assertTrue(PRIVATE.isEmpty());
  }

  @Test
  void testMethodScopeBeforeAllParameterFailsClearly() {
    run(MethodScopeBeforeAllFixture.class)
        .allEvents()
        .assertThatEvents()
        .haveExactly(1, event(finishedWithFailure(message(m -> m.contains("Scope.METHOD")))));
    Assertions.assertTrue(PRIVATE.isEmpty());
  }

  @Test
  void testStartPrivateIsUnsupportedByDefault() {
    UnsupportedOperationException e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> new KafkaSpec().startPrivate(ContainerSuite.getInstance()));

    Assertions.assertEquals(
        "scope CLASS/METHOD is not supported for KafkaSpec yet", e.getMessage());
  }

  private static EngineExecutionResults run(Class<?> fixture) {
    return EngineTestKit.engine("junit-jupiter")
        .selectors(DiscoverySelectors.selectClass(fixture))
        .enableImplicitConfigurationParameters(false)
        .execute();
  }

  // Only ever mocked, so the constructor never runs.
  abstract static class FakeContainer extends BaseContainer {
    FakeContainer() {
      super((String) null, null, null, null, null, null, Optional.empty());
    }
  }

  record FakeSpec(String name) implements ContainerSpec<FakeContainer> {
    @Override
    public Class<FakeContainer> containerType() {
      return FakeContainer.class;
    }

    @Override
    public FakeContainer getOrStart(ContainerSuite suite) {
      LOG.add("start " + name);
      return name.equals("first") ? FIRST : SECOND;
    }

    @Override
    public FakeContainer startPrivate(ContainerSuite suite) {
      LOG.add("startPrivate " + name);
      FakeContainer container = Mockito.mock(FakeContainer.class);
      PRIVATE.add(container);
      return container;
    }
  }

  // Only ever mocked, so the constructor never runs.
  abstract static class OtherContainer extends BaseContainer {
    OtherContainer() {
      super((String) null, null, null, null, null, null, Optional.empty());
    }
  }

  record OtherSpec() implements ContainerSpec<OtherContainer> {
    @Override
    public Class<OtherContainer> containerType() {
      return OtherContainer.class;
    }

    @Override
    public OtherContainer getOrStart(ContainerSuite suite) {
      LOG.add("start other");
      return OTHER;
    }
  }

  static final class FirstFactory implements ContainerFixture.SpecFactory<FakeFirstFixture> {
    @Override
    public ContainerSpec<?> specFor(FakeFirstFixture fixture) {
      return new FakeSpec(fixture.value());
    }

    @Override
    public Scope scopeFor(FakeFirstFixture fixture) {
      return fixture.scope();
    }

    @Override
    public Startup startupFor(FakeFirstFixture fixture) {
      return fixture.startup();
    }
  }

  static final class OtherFactory implements ContainerFixture.SpecFactory<FakeOtherFixture> {
    @Override
    public ContainerSpec<?> specFor(FakeOtherFixture fixture) {
      return new OtherSpec();
    }
  }

  static final class SecondFactory implements ContainerFixture.SpecFactory<FakeSecondFixture> {
    @Override
    public ContainerSpec<?> specFor(FakeSecondFixture fixture) {
      return new FakeSpec("second");
    }
  }

  @Target({ElementType.TYPE, ElementType.FIELD})
  @Retention(RetentionPolicy.RUNTIME)
  @ExtendWith(ContainerExtension.class)
  @ContainerFixture(FirstFactory.class)
  @interface FakeFirstFixture {
    String value() default "first";

    Scope scope() default Scope.RUN;

    Startup startup() default Startup.EAGER;
  }

  @Target({ElementType.TYPE, ElementType.FIELD})
  @Retention(RetentionPolicy.RUNTIME)
  @ExtendWith(ContainerExtension.class)
  @ContainerFixture(SecondFactory.class)
  @interface FakeSecondFixture {}

  @Target({ElementType.TYPE, ElementType.FIELD})
  @Retention(RetentionPolicy.RUNTIME)
  @ExtendWith(ContainerExtension.class)
  @ContainerFixture(OtherFactory.class)
  @interface FakeOtherFixture {}

  @Tag(FIXTURE)
  @FakeFirstFixture
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  static class InjectedFixture {
    @BeforeAll
    void beforeAll(FakeContainer container) {
      Assertions.assertSame(FIRST, container);
      LOG.add("beforeAll");
    }

    @Test
    void first(FakeContainer container) {
      Assertions.assertSame(FIRST, container);
      LOG.add("test");
    }

    @Test
    void second(BaseContainer container) {
      Assertions.assertSame(FIRST, container);
      LOG.add("test");
    }
  }

  // Shaped like BaseIT: the subclass overrides a no-argument @BeforeAll method.
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  static class SetUpBase {
    @BeforeAll
    void setUp() {}
  }

  @Tag(FIXTURE)
  @FakeFirstFixture
  static class FieldFixture extends SetUpBase {
    @FixtureContainer private FakeContainer container;

    @Override
    @BeforeAll
    void setUp() {
      Assertions.assertSame(FIRST, container);
      LOG.add("setUp");
    }

    @Test
    void test() {
      Assertions.assertSame(FIRST, container);
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture
  static class StaticFieldFixture {
    @FixtureContainer private static FakeContainer container;

    @BeforeAll
    static void setUp() {
      Assertions.assertSame(FIRST, container);
    }

    @Test
    void test() {
      Assertions.assertSame(FIRST, container);
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture
  static class FinalFieldFixture {
    @SuppressWarnings("UnusedVariable")
    @FixtureContainer
    private final FakeContainer container = null;

    @Test
    void test() {}
  }

  // Shaped like GvfsMultipleClusterIT: no class fixture, two fields of one type, read in an
  // overridden @BeforeAll.
  @Tag(FIXTURE)
  static class TwoFieldFixtures extends SetUpBase {
    @FakeFirstFixture @FixtureContainer private FakeContainer first;
    @FakeSecondFixture @FixtureContainer private FakeContainer second;

    @Override
    @BeforeAll
    void setUp() {
      Assertions.assertSame(FIRST, first);
      Assertions.assertSame(SECOND, second);
    }

    @Test
    void test() {
      Assertions.assertSame(FIRST, first);
      Assertions.assertSame(SECOND, second);
    }
  }

  @Tag(FIXTURE)
  static class TwoStaticFieldFixtures {
    @FakeFirstFixture @FixtureContainer private static FakeContainer first;
    @FakeSecondFixture @FixtureContainer private static FakeContainer second;

    @BeforeAll
    static void setUp() {
      Assertions.assertSame(FIRST, first);
      Assertions.assertSame(SECOND, second);
    }

    @Test
    void test() {
      Assertions.assertSame(SECOND, second);
    }
  }

  @Tag(FIXTURE)
  static class TwoFieldFixturesPerMethod {
    @FakeFirstFixture @FixtureContainer private FakeContainer first;
    @FakeSecondFixture @FixtureContainer private FakeContainer second;

    @Test
    void test() {
      Assertions.assertSame(FIRST, first);
      Assertions.assertSame(SECOND, second);
    }
  }

  @Tag(FIXTURE)
  static class WrongTypeFieldFixture {
    @SuppressWarnings("UnusedVariable")
    @FakeOtherFixture
    @FixtureContainer
    private static FakeContainer wrong;

    @Test
    void test() {}
  }

  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  static class LoggingSetUpBase {
    @BeforeAll
    void setUp() {
      LOG.add("setUp");
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture(startup = Startup.LAZY)
  static class LazyFixture extends LoggingSetUpBase {
    @BeforeAll
    void startAfterSetUp(FakeContainer container) {
      Assertions.assertSame(FIRST, container);
    }

    @Test
    void test() {
      LOG.add("test");
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture
  static class UnusedFixture {
    @Test
    void test() {}
  }

  @FakeFirstFixture
  abstract static class FixtureBase {}

  @Tag(FIXTURE)
  static class SubclassFixture extends FixtureBase {
    @Test
    void test(FakeContainer container) {
      Assertions.assertSame(FIRST, container);
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture("second")
  static class OverridingFixture extends FixtureBase {
    @Test
    void test(FakeContainer container) {
      Assertions.assertSame(SECOND, container);
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture
  @FakeSecondFixture
  static class AmbiguousFixture {
    @Test
    void test(FakeContainer container) {}
  }

  @Tag(FIXTURE)
  @KafkaFixture
  static class KafkaFixtureOnly {}

  @Tag(FIXTURE)
  @HiveFixture(version = HiveVersion.HIVE3, auth = HiveAuth.KERBEROS)
  static class HiveFixtureOnly {}

  @Tag(FIXTURE)
  @MySQLFixture(
      version = MySQLVersion.MYSQL_5_7,
      database = TestDatabaseName.MYSQL_CATALOG_MYSQL_IT)
  @PostgreSQLFixture(database = TestDatabaseName.PG_CATALOG_POSTGRESQL_IT)
  static class DatabaseFixturesOnly {}

  @Tag(FIXTURE)
  @FakeFirstFixture(scope = Scope.CLASS)
  static class ClassScopeFixture {
    @FixtureContainer private static FakeContainer staticField;
    @FixtureContainer private FakeContainer instanceField;

    @BeforeAll
    static void beforeAll(FakeContainer parameter) {
      Assertions.assertNotSame(FIRST, parameter);
      Assertions.assertSame(staticField, parameter);
    }

    @Test
    void first(FakeContainer parameter) {
      assertAllArePrivate(parameter);
    }

    @Test
    void second(FakeContainer parameter) {
      assertAllArePrivate(parameter);
    }

    private void assertAllArePrivate(FakeContainer parameter) {
      Assertions.assertSame(PRIVATE.get(0), staticField);
      Assertions.assertSame(staticField, instanceField);
      Assertions.assertSame(staticField, parameter);
    }
  }

  // One instance for both tests, so the field must be refilled before each test.
  @Tag(FIXTURE)
  @FakeFirstFixture(scope = Scope.METHOD)
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  static class MethodScopeFixture {
    @FixtureContainer private FakeContainer instanceField;

    @Test
    void first(FakeContainer parameter) {
      assertCurrent(parameter);
    }

    @Test
    void second(FakeContainer parameter) {
      assertCurrent(parameter);
    }

    private void assertCurrent(FakeContainer parameter) {
      Assertions.assertSame(PRIVATE.get(PRIVATE.size() - 1), instanceField);
      Assertions.assertSame(instanceField, parameter);
      LOG.add("test");
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture(scope = Scope.CLASS)
  @FakeOtherFixture
  static class MixedScopeFixture {
    @Test
    void test(FakeContainer fake, OtherContainer other) {
      Assertions.assertSame(PRIVATE.get(0), fake);
      Assertions.assertSame(OTHER, other);
    }
  }

  @Tag(FIXTURE)
  @FakeFirstFixture(scope = Scope.METHOD)
  static class MethodScopeStaticFieldFixture {
    @SuppressWarnings("UnusedVariable")
    @FixtureContainer
    private static FakeContainer staticField;

    @Test
    void test() {}
  }

  @Tag(FIXTURE)
  @FakeFirstFixture(scope = Scope.METHOD)
  static class MethodScopeBeforeAllFixture {
    @BeforeAll
    static void beforeAll(FakeContainer parameter) {}

    @Test
    void test() {}
  }
}
