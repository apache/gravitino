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
package org.apache.gravitino.authorization.ranger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.gravitino.authorization.ranger.integration.test.RangerBaseE2EIT;
import org.apache.gravitino.authorization.ranger.integration.test.RangerPaimonE2EIT;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Regression tests for Ranger policy refresh and temporary-role cleanup without containers. */
public class TestRangerPolicyRefresh {
  private static final String RANGER_EXTENSION =
      "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension";

  private SparkSession previousSession;
  private GravitinoMetalake previousMetalake;
  private String previousMetalakeName;

  @BeforeEach
  void saveSharedState() {
    previousSession = PolicyWaitProbe.session();
    previousMetalake = PolicyWaitProbe.metalake();
    previousMetalakeName = RangerBaseE2EIT.metalakeName;
    RangerBaseE2EIT.metalakeName = "metalake_policy_refresh_test";
    PolicyWaitProbe.session(null);
    PolicyWaitProbe.metalake(mock(GravitinoMetalake.class));
  }

  @AfterEach
  void restoreSharedState() {
    PolicyWaitProbe.session(previousSession);
    PolicyWaitProbe.metalake(previousMetalake);
    RangerBaseE2EIT.metalakeName = previousMetalakeName;
  }

  @Test
  void testNoSparkSessionDoesNotTouchPlugin() {
    try (MockedStatic<SparkRangerAdminPlugin> plugin = mockStatic(SparkRangerAdminPlugin.class)) {
      PolicyWaitProbe.waitForPolicies();
      plugin.verifyNoInteractions();
    }
  }

  @Test
  void testNonRangerSessionDoesNotTouchPlugin() {
    configureSession("org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions");
    try (MockedStatic<SparkRangerAdminPlugin> plugin = mockStatic(SparkRangerAdminPlugin.class)) {
      PolicyWaitProbe.waitForPolicies();
      plugin.verifyNoInteractions();
    }
  }

  @Test
  void testUninitializedPluginDoesNotRefresh() {
    configureSession(RANGER_EXTENSION);
    try (MockedStatic<SparkRangerAdminPlugin> plugin = mockStatic(SparkRangerAdminPlugin.class)) {
      PolicyWaitProbe.waitForPolicies();
      plugin.verify(SparkRangerAdminPlugin::getServiceDef);
      plugin.verify(SparkRangerAdminPlugin::refreshPoliciesAndTags, never());
    }
  }

  @Test
  void testInitializedPluginRefreshesWithCompatibleApi() {
    configureSession(
        RANGER_EXTENSION + ",org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions");
    try (MockedStatic<SparkRangerAdminPlugin> plugin = mockStatic(SparkRangerAdminPlugin.class)) {
      plugin.when(SparkRangerAdminPlugin::getServiceDef).thenReturn(new RangerServiceDef());
      PolicyWaitProbe.waitForPolicies();
      plugin.verify(SparkRangerAdminPlugin::getServiceDef);
      plugin.verify(SparkRangerAdminPlugin::refreshPoliciesAndTags);
    }
  }

  @Test
  void testRefreshFailureIsReported() {
    configureSession(RANGER_EXTENSION);
    IllegalStateException failure = new IllegalStateException("policy refresh failed");
    try (MockedStatic<SparkRangerAdminPlugin> plugin = mockStatic(SparkRangerAdminPlugin.class)) {
      plugin.when(SparkRangerAdminPlugin::getServiceDef).thenReturn(new RangerServiceDef());
      plugin.when(SparkRangerAdminPlugin::refreshPoliciesAndTags).thenThrow(failure);
      Assertions.assertSame(
          failure,
          Assertions.assertThrows(IllegalStateException.class, PolicyWaitProbe::waitForPolicies));
    }
  }

  @Test
  void testPaimonResetDeletesRoleAfterRefreshFailure() {
    IllegalStateException failure = new IllegalStateException("policy refresh failed");
    try (MockedStatic<RangerBaseE2EIT> policies = mockStatic(RangerBaseE2EIT.class)) {
      policies.when(PolicyWaitProbe::waitForPolicies).thenThrow(failure);
      Assertions.assertSame(
          failure, Assertions.assertThrows(IllegalStateException.class, () -> resetPaimon()));
      verify(PolicyWaitProbe.metalake()).deleteRole("reset");
    }
  }

  @Test
  void testPaimonResetDeletesRoleAfterCatalogFailure() {
    configureSession(RANGER_EXTENSION);
    IllegalStateException failure = new IllegalStateException("use catalog failed");
    when(PolicyWaitProbe.session().sql("USE paimon")).thenThrow(failure);
    try (MockedStatic<RangerBaseE2EIT> policies = mockStatic(RangerBaseE2EIT.class)) {
      Assertions.assertSame(
          failure, Assertions.assertThrows(IllegalStateException.class, () -> resetPaimon()));
      verify(PolicyWaitProbe.metalake()).deleteRole("reset");
    }
  }

  @Test
  void testPaimonResetDoesNotDeleteRoleWhenCreationFails() {
    RoleAlreadyExistsException failure = new RoleAlreadyExistsException("role exists");
    when(PolicyWaitProbe.metalake().createRole(eq("reset"), anyMap(), anyList()))
        .thenThrow(failure);
    Assertions.assertSame(
        failure, Assertions.assertThrows(RoleAlreadyExistsException.class, () -> resetPaimon()));
    verify(PolicyWaitProbe.metalake(), never()).deleteRole(any());
  }

  @Test
  void testPaimonResetPreservesRefreshFailureWhenCleanupFails() {
    IllegalStateException failure = new IllegalStateException("policy refresh failed");
    IllegalStateException cleanupFailure = new IllegalStateException("role cleanup failed");
    when(PolicyWaitProbe.metalake().deleteRole("reset")).thenThrow(cleanupFailure);
    try (MockedStatic<RangerBaseE2EIT> policies = mockStatic(RangerBaseE2EIT.class)) {
      policies.when(PolicyWaitProbe::waitForPolicies).thenThrow(failure);
      Assertions.assertSame(
          failure,
          Assertions.assertThrows(
              IllegalStateException.class, TestRangerPolicyRefresh::resetPaimon));
      Assertions.assertArrayEquals(new Throwable[] {cleanupFailure}, failure.getSuppressed());
    }
  }

  @Test
  void testPaimonResetCleansRoleAfterLinkageError() {
    NoSuchMethodError failure = new NoSuchMethodError("policy version method unavailable");
    try (MockedStatic<RangerBaseE2EIT> policies = mockStatic(RangerBaseE2EIT.class)) {
      policies.when(PolicyWaitProbe::waitForPolicies).thenThrow(failure);
      Assertions.assertSame(
          failure,
          Assertions.assertThrows(NoSuchMethodError.class, TestRangerPolicyRefresh::resetPaimon));
      verify(PolicyWaitProbe.metalake()).deleteRole("reset");
    }
  }

  @Test
  void testPaimonResetAvoidsSelfSuppression() {
    IllegalStateException failure = new IllegalStateException("shared failure");
    when(PolicyWaitProbe.metalake().deleteRole("reset")).thenThrow(failure);
    try (MockedStatic<RangerBaseE2EIT> policies = mockStatic(RangerBaseE2EIT.class)) {
      policies.when(PolicyWaitProbe::waitForPolicies).thenThrow(failure);
      Assertions.assertSame(
          failure,
          Assertions.assertThrows(
              IllegalStateException.class, TestRangerPolicyRefresh::resetPaimon));
      Assertions.assertEquals(0, failure.getSuppressed().length);
    }
  }

  @Test
  void testSuccessfulPaimonResetRefreshesAfterRoleDeletion() {
    configureSession(RANGER_EXTENSION);
    try (MockedStatic<RangerBaseE2EIT> policies = mockStatic(RangerBaseE2EIT.class)) {
      resetPaimon();
      verify(PolicyWaitProbe.metalake()).deleteRole("reset");
      policies.verify(PolicyWaitProbe::waitForPolicies, times(2));
    }
  }

  private void configureSession(String extensions) {
    SparkSession session = mock(SparkSession.class);
    SparkContext context = mock(SparkContext.class);
    when(session.sparkContext()).thenReturn(context);
    when(context.getConf())
        .thenReturn(new SparkConf(false).set("spark.sql.extensions", extensions));
    PolicyWaitProbe.session(session);
  }

  private static void resetPaimon() {
    try {
      Method method = RangerPaimonE2EIT.class.getDeclaredMethod("reset");
      method.setAccessible(true);
      method.invoke(new RangerPaimonE2EIT());
    } catch (InvocationTargetException e) {
      Throwables.throwIfUnchecked(e.getCause());
      throw new AssertionError(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  // Keep this probe abstract so it cannot run inherited E2E tests or their container lifecycle.
  private abstract static class PolicyWaitProbe extends RangerBaseE2EIT {
    static SparkSession session() {
      return sparkSession;
    }

    static void session(SparkSession session) {
      sparkSession = session;
    }

    static GravitinoMetalake metalake() {
      return metalake;
    }

    static void metalake(GravitinoMetalake value) {
      metalake = value;
    }

    static void waitForPolicies() {
      waitForUpdatingPolicies();
    }
  }
}
