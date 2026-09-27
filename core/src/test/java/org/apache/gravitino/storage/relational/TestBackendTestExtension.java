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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestWatcher;

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
  void testFailedSharedFixtureIsClosedAndRebuilt() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(DefaultIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    ExtensionContext firstContext = contexts.methodContext("first");
    Extension firstCallback = startInvocation(extension, true, firstContext);
    ((AfterEachCallback) firstCallback).afterEach(firstContext);
    ((TestWatcher) firstCallback).testFailed(firstContext, new AssertionError("expected"));

    runInvocation(extension, true, contexts.methodContext("second"));

    assertEquals(2, factory.backends.size());
    verify(factory.backends.get(0), times(1)).close();
    verify(factory.backends.get(1), never()).close();

    extension.afterAll(contexts.classContext);
    verify(factory.backends.get(1), times(1)).close();
  }

  @Test
  void testLifecycleFailurePoisonsSharedFixture() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(DefaultIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    ExtensionContext firstContext = contexts.methodContext("first");
    Extension firstCallback = startInvocation(extension, true, firstContext);
    assertThrows(
        IllegalStateException.class,
        () ->
            ((LifecycleMethodExecutionExceptionHandler) firstCallback)
                .handleBeforeEachMethodExecutionException(
                    firstContext, new IllegalStateException("expected")));

    runInvocation(extension, true, contexts.methodContext("second"));

    assertEquals(2, factory.backends.size());
    verify(factory.backends.get(0), times(1)).close();

    extension.afterAll(contexts.classContext);
    verify(factory.backends.get(1), times(1)).close();
  }

  @Test
  void testActivationFailurePoisonsSharedFixture() throws Exception {
    List<RelationalBackend> backends = new ArrayList<>();
    AtomicInteger activations = new AtomicInteger();
    BackendTestExtension.BackendFactory factory =
        backendType -> {
          RelationalBackend backend = mock(RelationalBackend.class);
          backends.add(backend);
          return new BackendTestExtension.BackendResource(
              backendType,
              backend,
              () -> {
                if (activations.incrementAndGet() == 2) {
                  throw new IllegalStateException("expected");
                }
              });
        };
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(DefaultIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, true, contexts.methodContext("first"));
    assertThrows(
        IllegalStateException.class,
        () -> startInvocation(extension, true, contexts.methodContext("second")));
    runInvocation(extension, true, contexts.methodContext("second"));

    assertEquals(2, backends.size());
    verify(backends.get(0), times(1)).close();

    extension.afterAll(contexts.classContext);
    verify(backends.get(1), times(1)).close();
  }

  @Test
  void testDedicatedServerIsolationFailsFast() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(DedicatedServerTests.class);
    extension.beforeAll(contexts.classContext);

    assertThrows(
        UnsupportedOperationException.class,
        () -> startInvocation(extension, true, contexts.methodContext("first")));
    assertEquals(0, factory.backends.size());

    extension.afterAll(contexts.classContext);
  }

  @Test
  void testAfterAllClosesEveryFixtureAndAggregatesFailures() throws Exception {
    CountingBackendFactory factory = new CountingBackendFactory();
    BackendTestExtension extension = new BackendTestExtension(factory);
    TestContexts contexts = new TestContexts(DefaultIsolationTests.class);
    extension.beforeAll(contexts.classContext);

    runInvocation(extension, true, "h2", contexts.methodContext("first"));
    runInvocation(extension, true, "mysql", contexts.methodContext("second"));
    doThrow(new IOException("h2 close")).when(factory.backends.get(0)).close();
    doThrow(new IOException("mysql close")).when(factory.backends.get(1)).close();

    Exception failure =
        assertThrows(Exception.class, () -> extension.afterAll(contexts.classContext));
    assertEquals(1, failure.getSuppressed().length);
    verify(factory.backends.get(0), times(1)).close();
    verify(factory.backends.get(1), times(1)).close();
  }

  @Test
  void testH2CleanupRunsAfterUncheckedBackendCloseFailure() throws Exception {
    BackendTestExtension.H2BackendWrapper backend =
        mock(BackendTestExtension.H2BackendWrapper.class);
    BackendTestExtension extension =
        new BackendTestExtension(
            backendType ->
                new BackendTestExtension.BackendResource(backendType, backend, () -> {}));
    TestContexts contexts = new TestContexts(DefaultIsolationTests.class);
    extension.beforeAll(contexts.classContext);
    runInvocation(extension, true, contexts.methodContext("first"));

    doThrow(new IllegalStateException("close")).when(backend).close();
    doThrow(new IOException("clean")).when(backend).cleanFile();

    Exception failure =
        assertThrows(Exception.class, () -> extension.afterAll(contexts.classContext));
    assertEquals("close", failure.getMessage());
    assertEquals(1, failure.getSuppressed().length);
    assertEquals("clean", failure.getSuppressed()[0].getMessage());
    verify(backend, times(1)).cleanFile();
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
    runInvocation(extension, reuseBackend, "h2", context);
  }

  private static void runInvocation(
      BackendTestExtension extension,
      boolean reuseBackend,
      String backendType,
      ExtensionContext context)
      throws Exception {
    Extension callback = startInvocation(extension, reuseBackend, backendType, context);
    ((AfterEachCallback) callback).afterEach(context);
  }

  private static Extension startInvocation(
      BackendTestExtension extension, boolean reuseBackend, ExtensionContext context)
      throws Exception {
    return startInvocation(extension, reuseBackend, "h2", context);
  }

  private static Extension startInvocation(
      BackendTestExtension extension,
      boolean reuseBackend,
      String backendType,
      ExtensionContext context)
      throws Exception {
    TestTemplateInvocationContext invocation =
        extension
            .createInvocationContexts("testMethod", List.of(backendType), reuseBackend)
            .findFirst()
            .orElseThrow();
    Extension callback = invocation.getAdditionalExtensions().get(0);
    ((BeforeEachCallback) callback).beforeEach(context);
    return callback;
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

  @DatabaseFixture(DatabaseIsolation.FRESH_NAMESPACE)
  private static class FreshIsolationTests {
    void first() {}

    void second() {}
  }

  private static class MixedIsolationTests {
    void resettable() {}

    @DatabaseFixture(DatabaseIsolation.FRESH_NAMESPACE)
    void fresh() {}
  }

  @DatabaseFixture(DatabaseIsolation.DEDICATED_SERVER)
  private static class DedicatedServerTests {
    void first() {}
  }
}
