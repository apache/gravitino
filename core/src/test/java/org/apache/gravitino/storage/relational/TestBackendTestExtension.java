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

package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

/**
 * Runs small fixture classes through a real JUnit engine and checks, from a log of what the
 * extension did, when it builds, resets and closes backends.
 */
class TestBackendTestExtension {

  /** Keeps the fixtures out of every Gradle test task; only this test runs them. */
  static final String FIXTURE = "gravitino-test-fixture";

  private static final List<String> LOG = new ArrayList<>();
  private static boolean failNextCreate;
  private static boolean failNextClose;

  @BeforeEach
  void clearLog() {
    LOG.clear();
    failNextCreate = false;
    failNextClose = false;
  }

  @Test
  void testClassReusesOneBackend() {
    run(SharedFixture.class).assertStatistics(stats -> stats.succeeded(2).failed(0));

    assertEquals(
        List.of(
            "create h2 SharedFixture",
            "reset DELETE_ROWS",
            "reset DELETE_ROWS",
            "close h2 SharedFixture"),
        LOG);
  }

  @Test
  void testEachClassClosesItsBackendBeforeTheNextStarts() {
    run(SharedFixture.class, TruncateFixture.class)
        .assertStatistics(stats -> stats.succeeded(3).failed(0));

    assertEquals(
        List.of(
            "create h2 SharedFixture",
            "reset DELETE_ROWS",
            "reset DELETE_ROWS",
            "close h2 SharedFixture",
            "create h2 TruncateFixture",
            "reset TRUNCATE_TABLES",
            "close h2 TruncateFixture"),
        LOG);
  }

  @Test
  void testBackendTypeChangeReplacesTheBackend() {
    run(TwoBackendFixture.class).assertStatistics(stats -> stats.succeeded(2).failed(0));

    assertEquals(
        List.of(
            "create h2 TwoBackendFixture",
            "reset DELETE_ROWS",
            "close h2 TwoBackendFixture",
            "create mysql TwoBackendFixture",
            "reset DELETE_ROWS",
            "close mysql TwoBackendFixture"),
        LOG);
  }

  @Test
  void testFreshClassGetsABackendPerTest() {
    run(FreshFixture.class).assertStatistics(stats -> stats.succeeded(2).failed(0));

    assertEquals(
        List.of(
            "create h2 FreshFixture",
            "reset FRESH_DATABASE",
            "close h2 FreshFixture",
            "create h2 FreshFixture",
            "reset FRESH_DATABASE",
            "close h2 FreshFixture"),
        LOG);
  }

  @Test
  void testSubclassInheritsFreshDatabase() {
    run(InheritedFreshFixture.class).assertStatistics(stats -> stats.succeeded(3).failed(0));

    assertEquals(3, count("create h2 InheritedFreshFixture"));
    assertEquals(3, count("reset FRESH_DATABASE"));
    assertEquals(3, count("close h2 InheritedFreshFixture"));
  }

  @Test
  void testFreshMethodReplacesTheSharedBackend() {
    run(MixedFixture.class).assertStatistics(stats -> stats.succeeded(3).failed(0));

    assertEquals(
        List.of(
            "create h2 MixedFixture",
            "reset DELETE_ROWS",
            "close h2 MixedFixture",
            "create h2 MixedFixture",
            "reset FRESH_DATABASE",
            "close h2 MixedFixture",
            "create h2 MixedFixture",
            "reset DELETE_ROWS",
            "close h2 MixedFixture"),
        LOG);
  }

  @Test
  void testSharedMethodInsideFreshClass() {
    run(FreshClassWithSharedMethodFixture.class)
        .assertStatistics(stats -> stats.succeeded(3).failed(0));

    assertEquals(
        List.of(
            "create h2 FreshClassWithSharedMethodFixture",
            "reset DELETE_ROWS",
            "reset DELETE_ROWS",
            "close h2 FreshClassWithSharedMethodFixture",
            "create h2 FreshClassWithSharedMethodFixture",
            "reset FRESH_DATABASE",
            "close h2 FreshClassWithSharedMethodFixture"),
        LOG);
  }

  @Test
  void testFailedCreateFailsOnlyThatTest() {
    failNextCreate = true;
    run(SharedFixture.class).assertStatistics(stats -> stats.succeeded(1).failed(1));

    assertEquals(
        List.of(
            "create failed",
            "create h2 SharedFixture",
            "reset DELETE_ROWS",
            "close h2 SharedFixture"),
        LOG);
  }

  @Test
  void testFailedCloseOnReplacementFailsTheReplacingTest() {
    failNextClose = true;
    run(TwoBackendFixture.class).assertStatistics(stats -> stats.succeeded(1).failed(1));

    // The failed close leaves the slot empty, so nothing is left to close at the end.
    assertEquals(
        List.of("create h2 TwoBackendFixture", "reset DELETE_ROWS", "close h2 TwoBackendFixture"),
        LOG);
  }

  @Test
  void testDisplayNameNamesTheBackend() {
    List<String> names =
        run(TwoBackendFixture.class).started().stream()
            .map(event -> event.getTestDescriptor().getDisplayName())
            .collect(Collectors.toList());

    assertEquals(List.of("first[H2 Backend]", "first[MYSQL Backend]"), names);
  }

  private static Events run(Class<?>... fixtures) {
    return EngineTestKit.engine("junit-jupiter")
        .configurationParameter(
            "junit.jupiter.testclass.order.default", "org.junit.jupiter.api.ClassOrderer$ClassName")
        .selectors(
            Arrays.stream(fixtures)
                .map(DiscoverySelectors::selectClass)
                .toArray(DiscoverySelector[]::new))
        .execute()
        .testEvents();
  }

  private static long count(String entry) {
    return LOG.stream().filter(entry::equals).count();
  }

  private static BackendTestExtension fakeExtension(String... backends) {
    return new BackendTestExtension(
        (backendType, testClass) -> {
          if (failNextCreate) {
            failNextCreate = false;
            LOG.add("create failed");
            throw new IllegalStateException("create failed");
          }
          String name = backendType + " " + testClass.getSimpleName();
          LOG.add("create " + name);
          return new BackendTestExtension.Backend(
              backendType, testClass, mock(RelationalBackend.class), null, null) {
            @Override
            void reset(ResetStrategy resetStrategy) {
              LOG.add("reset " + resetStrategy);
            }

            @Override
            public void close() throws IOException {
              LOG.add("close " + name);
              if (failNextClose) {
                failNextClose = false;
                throw new IOException("close failed");
              }
            }
          };
        }) {
      @Override
      List<String> backends() {
        return List.of(backends);
      }
    };
  }

  @Tag(FIXTURE)
  @TestMethodOrder(MethodOrderer.MethodName.class)
  static class SharedFixture {
    @RegisterExtension static final BackendTestExtension EXTENSION = fakeExtension("h2");

    @TestTemplate
    void first() {}

    @TestTemplate
    void second() {}
  }

  @Tag(FIXTURE)
  @DatabaseFixture(resetStrategy = ResetStrategy.TRUNCATE_TABLES)
  static class TruncateFixture {
    @RegisterExtension static final BackendTestExtension EXTENSION = fakeExtension("h2");

    @TestTemplate
    void first() {}
  }

  @Tag(FIXTURE)
  static class TwoBackendFixture {
    @RegisterExtension static final BackendTestExtension EXTENSION = fakeExtension("h2", "mysql");

    @TestTemplate
    void first() {}
  }

  @Tag(FIXTURE)
  @TestMethodOrder(MethodOrderer.MethodName.class)
  @DatabaseFixture(resetStrategy = ResetStrategy.FRESH_DATABASE)
  static class FreshFixture {
    @RegisterExtension static final BackendTestExtension EXTENSION = fakeExtension("h2");

    @TestTemplate
    void first() {}

    @TestTemplate
    void second() {}
  }

  @Tag(FIXTURE)
  @TestMethodOrder(MethodOrderer.MethodName.class)
  static class InheritedFreshFixture extends FreshFixture {
    @TestTemplate
    void third() {}
  }

  @Tag(FIXTURE)
  @TestMethodOrder(MethodOrderer.MethodName.class)
  static class MixedFixture {
    @RegisterExtension static final BackendTestExtension EXTENSION = fakeExtension("h2");

    @TestTemplate
    void a() {}

    @TestTemplate
    @DatabaseFixture(resetStrategy = ResetStrategy.FRESH_DATABASE)
    void b() {}

    @TestTemplate
    void c() {}
  }

  @Tag(FIXTURE)
  @TestMethodOrder(MethodOrderer.MethodName.class)
  @DatabaseFixture(resetStrategy = ResetStrategy.FRESH_DATABASE)
  static class FreshClassWithSharedMethodFixture {
    @RegisterExtension static final BackendTestExtension EXTENSION = fakeExtension("h2");

    @TestTemplate
    @DatabaseFixture(resetStrategy = ResetStrategy.DELETE_ROWS)
    void a() {}

    @TestTemplate
    @DatabaseFixture(resetStrategy = ResetStrategy.DELETE_ROWS)
    void b() {}

    @TestTemplate
    void c() {}
  }
}
