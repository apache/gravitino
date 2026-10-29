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
import java.util.List;
import java.util.Optional;
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

  @BeforeEach
  void clearLog() {
    LOG.clear();
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
  }

  static final class FirstFactory implements ContainerFixture.SpecFactory<FakeFirstFixture> {
    @Override
    public ContainerSpec<?> specFor(FakeFirstFixture fixture) {
      return new FakeSpec("first");
    }
  }

  static final class SecondFactory implements ContainerFixture.SpecFactory<FakeSecondFixture> {
    @Override
    public ContainerSpec<?> specFor(FakeSecondFixture fixture) {
      return new FakeSpec("second");
    }
  }

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @ExtendWith(ContainerExtension.class)
  @ContainerFixture(FirstFactory.class)
  @interface FakeFirstFixture {}

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @ExtendWith(ContainerExtension.class)
  @ContainerFixture(SecondFactory.class)
  @interface FakeSecondFixture {}

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
  @FakeFirstFixture
  @FakeSecondFixture
  static class AmbiguousFixture {
    @Test
    void test(FakeContainer container) {}
  }

  @Tag(FIXTURE)
  @KafkaFixture
  static class KafkaFixtureOnly {}
}
