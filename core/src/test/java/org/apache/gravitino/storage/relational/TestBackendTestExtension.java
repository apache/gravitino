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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

class TestBackendTestExtension {

  private final List<RelationalBackend> backends = new ArrayList<>();
  private final BackendTestExtension extension =
      new BackendTestExtension(
          (backendType, testClass) -> {
            RelationalBackend backend = mock(RelationalBackend.class);
            backends.add(backend);
            return new BackendTestExtension.Backend(backendType, testClass, backend, null, null);
          });
  private final ExtensionContext root = rootWithStore();

  @Test
  void testClassReusesOneBackend() throws Exception {
    run(SharedTests.class, "first", "h2");
    run(SharedTests.class, "second", "h2");

    assertEquals(1, backends.size());
    verify(backends.get(0), never()).close();
  }

  @Test
  void testNextClassGetsANewBackend() throws Exception {
    run(SharedTests.class, "first", "h2");
    run(OtherSharedTests.class, "first", "h2");

    assertEquals(2, backends.size());
    verify(backends.get(0), times(1)).close();
    verify(backends.get(1), never()).close();
  }

  @Test
  void testBackendTypeChangeGetsANewBackend() throws Exception {
    run(SharedTests.class, "first", "h2");
    run(SharedTests.class, "first", "mysql");

    assertEquals(2, backends.size());
    verify(backends.get(0), times(1)).close();
  }

  @Test
  void testFreshClassGetsABackendPerTest() throws Exception {
    run(FreshTests.class, "first", "h2");
    run(FreshTests.class, "second", "h2");

    assertEquals(2, backends.size());
    verify(backends.get(0), times(1)).close();
    verify(backends.get(1), times(1)).close();
  }

  @Test
  void testFreshMethodReplacesTheSharedBackend() throws Exception {
    run(MixedTests.class, "shared", "h2");
    run(MixedTests.class, "fresh", "h2");
    run(MixedTests.class, "shared", "h2");

    assertEquals(3, backends.size());
    verify(backends.get(0), times(1)).close();
    verify(backends.get(1), times(1)).close();
    verify(backends.get(2), never()).close();
  }

  @Test
  void testResetTablesMethodSharesInsideFreshClass() throws Exception {
    run(FreshClassWithSharedMethodTests.class, "shared", "h2");
    run(FreshClassWithSharedMethodTests.class, "shared", "h2");
    assertEquals(1, backends.size());

    run(FreshClassWithSharedMethodTests.class, "fresh", "h2");
    assertEquals(2, backends.size());
    verify(backends.get(0), times(1)).close();
    verify(backends.get(1), times(1)).close();
  }

  @Test
  void testSubclassInheritsFreshDatabase() throws Exception {
    run(InheritedFreshTests.class, "third", "h2");
    run(InheritedFreshTests.class, "fourth", "h2");

    assertEquals(2, backends.size());
    verify(backends.get(0), times(1)).close();
    verify(backends.get(1), times(1)).close();
  }

  @Test
  void testResetModeReachesTheTest() throws Exception {
    TestJDBCBackend test = mock(TestJDBCBackend.class);
    run(SharedTests.class, "first", "h2", test);
    run(TruncateTests.class, "first", "h2", test);

    verify(test).setResetStrategy(ResetStrategy.DELETE_ROWS);
    verify(test).setResetStrategy(ResetStrategy.TRUNCATE_TABLES);
  }

  @Test
  void testDisplayNamePreservesMethodAndBackend() {
    TestTemplateInvocationContext context =
        extension.createInvocationContexts("testMethod", List.of("h2")).findFirst().orElseThrow();

    assertEquals("testMethod[H2 Backend]", context.getDisplayName(1));
  }

  private void run(Class<?> testClass, String methodName, String backendType) throws Exception {
    run(testClass, methodName, backendType, this);
  }

  private void run(Class<?> testClass, String methodName, String backendType, Object testInstance)
      throws Exception {
    Method method = testClass.getDeclaredMethod(methodName);
    ExtensionContext context = mock(ExtensionContext.class);
    when(context.getRoot()).thenReturn(root);
    when(context.getRequiredTestMethod()).thenReturn(method);
    when(context.getRequiredTestClass()).thenAnswer(invocation -> testClass);
    when(context.getRequiredTestInstance()).thenReturn(testInstance);

    Extension callback =
        extension
            .createInvocationContexts(methodName, List.of(backendType))
            .findFirst()
            .orElseThrow()
            .getAdditionalExtensions()
            .get(0);
    ((BeforeEachCallback) callback).beforeEach(context);
    ((AfterEachCallback) callback).afterEach(context);
  }

  private static ExtensionContext rootWithStore() {
    Map<Object, Object> entries = new HashMap<>();
    ExtensionContext.Store store = mock(ExtensionContext.Store.class);
    doAnswer(invocation -> entries.put(invocation.getArgument(0), invocation.getArgument(1)))
        .when(store)
        .put(any(), any());
    when(store.get(any(), any(Class.class)))
        .thenAnswer(invocation -> entries.get(invocation.getArgument(0)));
    when(store.remove(any(), any(Class.class)))
        .thenAnswer(invocation -> entries.remove(invocation.getArgument(0)));
    ExtensionContext root = mock(ExtensionContext.class);
    when(root.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
    return root;
  }

  private static class SharedTests {
    void first() {}

    void second() {}
  }

  private static class OtherSharedTests {
    void first() {}
  }

  @DatabaseFixture(resetStrategy = ResetStrategy.FRESH_DATABASE)
  private static class FreshTests {
    void first() {}

    void second() {}
  }

  private static class InheritedFreshTests extends FreshTests {
    void third() {}

    void fourth() {}
  }

  private static class MixedTests {
    void shared() {}

    @DatabaseFixture(resetStrategy = ResetStrategy.FRESH_DATABASE)
    void fresh() {}
  }

  @DatabaseFixture(resetStrategy = ResetStrategy.FRESH_DATABASE)
  private static class FreshClassWithSharedMethodTests {
    @DatabaseFixture(resetStrategy = ResetStrategy.DELETE_ROWS)
    void shared() {}

    void fresh() {}
  }

  @DatabaseFixture(resetStrategy = ResetStrategy.TRUNCATE_TABLES)
  private static class TruncateTests {
    void first() {}
  }
}
