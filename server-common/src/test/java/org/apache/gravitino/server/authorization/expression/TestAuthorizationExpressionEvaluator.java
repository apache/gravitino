/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.expression;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import ognl.OgnlException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.MockGravitinoAuthorizer;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test for {@link AuthorizationExpressionEvaluator} */
public class TestAuthorizationExpressionEvaluator {

  @BeforeEach
  public void clearCache() {
    AuthorizationExpressionEvaluator.clearParsedExpressionCache();
  }

  @Test
  public void testEvaluator() {
    String expression =
        "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA && (TABLE::SELECT_TABLE || TABLE::MODIFY_TABLE)";
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());
      AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
          new AuthorizationExpressionEvaluator(expression);

      Map<Entity.EntityType, NameIdentifier> metadataNames = new HashMap<>();
      metadataNames.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("testMetalake"));
      metadataNames.put(
          Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog("testMetalake", "testCatalog"));
      metadataNames.put(
          Entity.EntityType.SCHEMA,
          NameIdentifierUtil.ofSchema("testMetalake", "testCatalog", "testSchema"));
      metadataNames.put(
          Entity.EntityType.TABLE,
          NameIdentifierUtil.ofTable(
              "testMetalake", "testCatalog", "testSchema", "testTableHasNotPermission"));
      Assertions.assertFalse(
          authorizationExpressionEvaluator.evaluate(
              metadataNames, new AuthorizationRequestContext()));
      metadataNames.put(
          Entity.EntityType.TABLE,
          NameIdentifierUtil.ofTable("testMetalake", "testCatalog", "testSchema", "testTable"));
      Assertions.assertTrue(
          authorizationExpressionEvaluator.evaluate(
              metadataNames, new AuthorizationRequestContext()));
    }
  }

  @Test
  public void testEvaluatorWithOwner() {
    String expression = "METALAKE::OWNER || CATALOG::CREATE_CATALOG";
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());

      AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
          new AuthorizationExpressionEvaluator(expression);
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));

      Map<Entity.EntityType, NameIdentifier> metadataNames = new HashMap<>();
      metadataNames.put(
          Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("metalakeWithOutOwner"));
      metadataNames.put(
          Entity.EntityType.CATALOG,
          NameIdentifierUtil.ofCatalog("metalakeWithOwner", "testCatalog"));
      Assertions.assertFalse(
          authorizationExpressionEvaluator.evaluate(
              metadataNames, new AuthorizationRequestContext()));
      metadataNames.put(
          Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("metalakeWithOwner"));
      Assertions.assertTrue(
          authorizationExpressionEvaluator.evaluate(
              metadataNames, new AuthorizationRequestContext()));
    }
  }

  @Test
  public void testParsedExpressionTreeIsReused() {
    String expression = "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA";
    try (MockedStatic<GravitinoAuthorizerProvider> mockStatic =
        mockStatic(GravitinoAuthorizerProvider.class)) {
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());

      AuthorizationExpressionEvaluator first = new AuthorizationExpressionEvaluator(expression);
      AuthorizationExpressionEvaluator second = new AuthorizationExpressionEvaluator(expression);
      Assertions.assertSame(
          first.getOgnlAuthorizationExpressionTree(),
          second.getOgnlAuthorizationExpressionTree(),
          "The same expression should reuse one cached parsed AST");

      AuthorizationExpressionEvaluator other =
          new AuthorizationExpressionEvaluator("CATALOG::USE_CATALOG");
      Assertions.assertNotSame(
          first.getOgnlAuthorizationExpressionTree(),
          other.getOgnlAuthorizationExpressionTree(),
          "Different expressions should not share a parsed AST");
    }
  }

  @Test
  public void testConcurrentEvaluationOnSharedTree() throws Exception {
    String expression =
        "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA && (TABLE::SELECT_TABLE || TABLE::MODIFY_TABLE)";
    // No static mocks here: the evaluate overload takes an explicit principal and the constructor
    // an
    // explicit authorizer, so worker threads never depend on thread-confined Mockito static mocks.
    UserPrincipal principal = new UserPrincipal("tester");
    AuthorizationExpressionEvaluator evaluator =
        new AuthorizationExpressionEvaluator(expression, new MockGravitinoAuthorizer());

    int threads = 16;
    int iterations = 200;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch startGate = new CountDownLatch(1);
    AtomicInteger failures = new AtomicInteger();
    AtomicReference<Throwable> firstError = new AtomicReference<>();
    Future<?>[] results = new Future<?>[threads];
    for (int t = 0; t < threads; t++) {
      boolean expectAuthorized = t % 2 == 0;
      results[t] =
          pool.submit(
              () -> {
                try {
                  startGate.await();
                  for (int i = 0; i < iterations; i++) {
                    boolean actual =
                        evaluator.evaluate(
                            metadataNames(expectAuthorized),
                            new AuthorizationRequestContext(),
                            principal,
                            Optional.empty());
                    if (actual != expectAuthorized) {
                      failures.incrementAndGet();
                    }
                  }
                } catch (Throwable e) {
                  failures.incrementAndGet();
                  firstError.compareAndSet(null, e);
                }
              });
    }
    startGate.countDown();
    for (Future<?> result : results) {
      result.get(30, TimeUnit.SECONDS);
    }
    pool.shutdownNow();
    Assertions.assertEquals(
        0,
        failures.get(),
        "Concurrent evaluation on a shared AST produced wrong or failed results; first error: "
            + firstError.get());
  }

  @Test
  public void testConstructorFailsFastOnInvalidExpression() {
    try (MockedStatic<AuthorizationExpressionConverter> converterMocked =
        mockStatic(AuthorizationExpressionConverter.class)) {
      converterMocked
          .when(() -> AuthorizationExpressionConverter.convertToOgnlExpression("BAD"))
          .thenReturn("a b c ((");
      RuntimeException e =
          Assertions.assertThrows(
              RuntimeException.class,
              () -> new AuthorizationExpressionEvaluator("BAD", new MockGravitinoAuthorizer()));
      Assertions.assertInstanceOf(OgnlException.class, e.getCause());
    }
  }

  private static Map<Entity.EntityType, NameIdentifier> metadataNames(boolean authorized) {
    Map<Entity.EntityType, NameIdentifier> metadataNames = new HashMap<>();
    metadataNames.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("testMetalake"));
    metadataNames.put(
        Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog("testMetalake", "testCatalog"));
    metadataNames.put(
        Entity.EntityType.SCHEMA,
        NameIdentifierUtil.ofSchema("testMetalake", "testCatalog", "testSchema"));
    metadataNames.put(
        Entity.EntityType.TABLE,
        NameIdentifierUtil.ofTable(
            "testMetalake",
            "testCatalog",
            "testSchema",
            authorized ? "testTable" : "testTableHasNotPermission"));
    return metadataNames;
  }
}
