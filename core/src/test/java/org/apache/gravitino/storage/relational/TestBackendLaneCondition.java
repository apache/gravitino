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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Direct unit test of {@link BackendLaneCondition}, independent of any real Gradle lane execution:
 * exercises the exact matrix that used to be encoded across three separate hand-maintained {@code
 * excludeTags(...)} clauses in {@code core/build.gradle.kts}.
 */
class TestBackendLaneCondition {

  private static final String BACKEND_PROPERTY = "gravitino.core.test.backend";

  private final BackendLaneCondition condition = new BackendLaneCondition();

  private String previousBackendProperty;

  @DatabaseTest
  private static class AllBackendsFixture {}

  @DatabaseTest(backends = DatabaseBackend.H2)
  private static class H2OnlyFixture {}

  @DatabaseTest(backends = {DatabaseBackend.MYSQL, DatabaseBackend.POSTGRESQL})
  private static class MySQLAndPostgreSQLFixture {}

  private static class NotDatabaseTestFixture {}

  @BeforeEach
  void saveBackendProperty() {
    previousBackendProperty = System.getProperty(BACKEND_PROPERTY);
  }

  @AfterEach
  void restoreBackendProperty() {
    if (previousBackendProperty == null) {
      System.clearProperty(BACKEND_PROPERTY);
    } else {
      System.setProperty(BACKEND_PROPERTY, previousBackendProperty);
    }
  }

  @Test
  void nonDatabaseTestClassIsAlwaysEnabled() {
    System.setProperty(BACKEND_PROPERTY, "mysql");
    assertTrue(evaluate(NotDatabaseTestFixture.class));
  }

  @Test
  void noBackendSelectedRunsUnderEveryDeclaredBackend() {
    System.clearProperty(BACKEND_PROPERTY);
    assertTrue(evaluate(H2OnlyFixture.class));
    assertTrue(evaluate(MySQLAndPostgreSQLFixture.class));
    assertTrue(evaluate(AllBackendsFixture.class));
  }

  @Test
  void allBackendsFixtureIsEnabledUnderEveryLane() {
    for (String backend : new String[] {"h2", "mysql", "postgresql"}) {
      System.setProperty(BACKEND_PROPERTY, backend);
      assertTrue(evaluate(AllBackendsFixture.class), "expected enabled for " + backend);
    }
  }

  @Test
  void h2OnlyFixtureRunsOnlyInTheH2Lane() {
    System.setProperty(BACKEND_PROPERTY, "h2");
    assertTrue(evaluate(H2OnlyFixture.class));

    System.setProperty(BACKEND_PROPERTY, "mysql");
    assertFalse(evaluate(H2OnlyFixture.class));

    System.setProperty(BACKEND_PROPERTY, "postgresql");
    assertFalse(evaluate(H2OnlyFixture.class));
  }

  @Test
  void multiBackendFixtureRunsOnlyInItsDeclaredLanes() {
    System.setProperty(BACKEND_PROPERTY, "h2");
    assertFalse(evaluate(MySQLAndPostgreSQLFixture.class));

    System.setProperty(BACKEND_PROPERTY, "mysql");
    assertTrue(evaluate(MySQLAndPostgreSQLFixture.class));

    System.setProperty(BACKEND_PROPERTY, "postgresql");
    assertTrue(evaluate(MySQLAndPostgreSQLFixture.class));
  }

  private boolean evaluate(Class<?> testClass) {
    ExtensionContext context = mock(ExtensionContext.class);
    when(context.getTestClass()).thenReturn(Optional.of(testClass));
    return !condition.evaluateExecutionCondition(context).isDisabled();
  }
}
