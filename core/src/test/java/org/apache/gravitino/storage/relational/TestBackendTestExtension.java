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
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

class TestBackendTestExtension {

  @Test
  void testSelectedBackendReusesClassFixture() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(DefaultIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, true, contexts.methodContext("first"));
    runInvocation(extension, true, contexts.methodContext("second"));

    assertEquals(1, factory.backends.size());
    assertEquals(2, factory.activationCount);
    verify(factory.backends.get(0), never()).close();

    extension.afterAll(contexts.classContext);
    verify(factory.backends.get(0), times(1)).close();
  }

  @Test
  void testLegacyH2DoesNotReuseFixture() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(DefaultIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, false, contexts.methodContext("first"));
    runInvocation(extension, false, contexts.methodContext("second"));

    assertEquals(2, factory.backends.size());
    verify(factory.backends.get(0), times(1)).close();
    verify(factory.backends.get(1), times(1)).close();

    extension.afterAll(contexts.classContext);
  }

  @Test
  void testFreshClassCreatesAndClosesEachInvocation() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(FreshIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, true, contexts.methodContext("first"));
    runInvocation(extension, true, contexts.methodContext("second"));

    assertEquals(2, factory.backends.size());
    verify(factory.backends.get(0), times(1)).close();
    verify(factory.backends.get(1), times(1)).close();

    extension.afterAll(contexts.classContext);
  }

  @Test
  void testFreshMethodEvictsSharedFixture() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(MixedIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, true, contexts.methodContext("resettable"));
    runInvocation(extension, true, contexts.methodContext("fresh"));
    runInvocation(extension, true, contexts.methodContext("resettable"));

    assertEquals(3, factory.backends.size());
    verify(factory.backends.get(0), times(1)).close();
    verify(factory.backends.get(1), times(1)).close();
    verify(factory.backends.get(2), never()).close();

    extension.afterAll(contexts.classContext);
    verify(factory.backends.get(2), times(1)).close();
  }

  @Test
  void testResetTablesMethodSharesInsideFreshClass() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(FreshClassWithSharedMethodTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, true, contexts.methodContext("shared"));
    runInvocation(extension, true, contexts.methodContext("shared"));
    assertEquals(1, factory.backends.size());
    verify(factory.backends.get(0), never()).close();

    runInvocation(extension, true, contexts.methodContext("fresh"));
    assertEquals(2, factory.backends.size());
    verify(factory.backends.get(0), times(1)).close();
    verify(factory.backends.get(1), times(1)).close();

    extension.afterAll(contexts.classContext);
  }

  @Test
  void testSubclassInheritsFreshDatabase() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(InheritedFreshTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, true, contexts.methodContext("third"));
    runInvocation(extension, true, contexts.methodContext("fourth"));

    assertEquals(2, factory.backends.size());
    verify(factory.backends.get(0), times(1)).close();
    verify(factory.backends.get(1), times(1)).close();

    extension.afterAll(contexts.classContext);
  }

  @Test
  void testDisplayNamePreservesMethodAndBackend() {
    BackendTestExtension extension =
        new BackendTestExtension(
            backendType ->
                new BackendTestExtension.BackendResource(
                    backendType, mock(RelationalBackend.class), () -> {}));

    TestTemplateInvocationContext context =
        extension
            .createInvocationContexts("testMethod", List.of("h2"), true)
            .findFirst()
            .orElseThrow();

    assertEquals("testMethod[H2 Backend]", context.getDisplayName(1));
  }

  private static void runInvocation(
      BackendTestExtension extension, boolean reuseBackend, ExtensionContext context)
      throws Exception {
    TestTemplateInvocationContext invocation =
        extension
            .createInvocationContexts("testMethod", List.of("h2"), reuseBackend)
            .findFirst()
            .orElseThrow();
    Extension callback = invocation.getAdditionalExtensions().get(0);
    ((BeforeEachCallback) callback).beforeEach(context);
    ((AfterEachCallback) callback).afterEach(context);
  }

  private static class CountingBackendFactory implements BackendTestExtension.BackendFactory {
    private final List<RelationalBackend> backends = new ArrayList<>();
    private int activationCount;

    @Override
    public BackendTestExtension.BackendResource create(String backendType) {
      RelationalBackend backend = mock(RelationalBackend.class);
      backends.add(backend);
      return new BackendTestExtension.BackendResource(
          backendType, backend, () -> activationCount++);
    }
  }

  private static class TestContexts {
    private final Class<?> testClass;
    private final ExtensionContext classContext;

    private TestContexts(Class<?> testClass) {
      this.testClass = testClass;
      classContext = mock(ExtensionContext.class);
      ExtensionContext.Store store = mock(ExtensionContext.Store.class);
      Map<Object, Object> entries = new HashMap<>();

      doAnswer(
              invocation -> {
                entries.put(invocation.getArgument(0), invocation.getArgument(1));
                return null;
              })
          .when(store)
          .put(any(), any());
      when(store.get(any())).thenAnswer(invocation -> entries.get(invocation.getArgument(0)));
      when(store.remove(any())).thenAnswer(invocation -> entries.remove(invocation.getArgument(0)));
      when(classContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
      when(classContext.getTestMethod()).thenReturn(Optional.empty());
      when(classContext.getRequiredTestClass()).thenAnswer(invocation -> testClass);
    }

    private ExtensionContext methodContext(String methodName) throws NoSuchMethodException {
      Method method = testClass.getDeclaredMethod(methodName);
      ExtensionContext methodContext = mock(ExtensionContext.class);
      when(methodContext.getTestMethod()).thenReturn(Optional.of(method));
      when(methodContext.getRequiredTestMethod()).thenReturn(method);
      when(methodContext.getRequiredTestClass()).thenAnswer(invocation -> testClass);
      when(methodContext.getRequiredTestInstance()).thenReturn(this);
      when(methodContext.getParent()).thenReturn(Optional.of(classContext));
      return methodContext;
    }
  }

  private static class DefaultIsolationTests {
    void first() {}

    void second() {}
  }

  @DatabaseFixture(isolation = DatabaseIsolation.FRESH_DATABASE)
  private static class FreshIsolationTests {
    void first() {}

    void second() {}
  }

  private static class MixedIsolationTests {
    void resettable() {}

    @DatabaseFixture(isolation = DatabaseIsolation.FRESH_DATABASE)
    void fresh() {}
  }

  private static class InheritedFreshTests extends FreshIsolationTests {
    void third() {}

    void fourth() {}
  }

  @DatabaseFixture(isolation = DatabaseIsolation.FRESH_DATABASE)
  private static class FreshClassWithSharedMethodTests {
    @DatabaseFixture(isolation = DatabaseIsolation.RESET_TABLES)
    void shared() {}

    void fresh() {}
  }
}
