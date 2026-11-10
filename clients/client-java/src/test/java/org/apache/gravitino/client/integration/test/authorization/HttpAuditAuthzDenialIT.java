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

package org.apache.gravitino.client.integration.test.authorization;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.integration.test.CapturedEventListener;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.server.AuthorizationDenialFailureEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Integration tests for authorization denial scenarios in {@code
 * MetadataAuthorizationMethodInterceptor} and their interaction with {@code HttpAuditFilter}. All
 * tests use MiniGravitino with authorization enabled.
 *
 * <p>Two denial mechanisms are covered:
 *
 * <ul>
 *   <li><b>Executor denial</b>: the user is a metalake member but lacks the required privilege. The
 *       authorization executor returns {@code false}, triggering an {@link
 *       AuthorizationDenialFailureEvent}. The {@code operationFailureFired} flag prevents {@code
 *       HttpAuditFilter} from emitting a duplicate failure event.
 *   <li><b>User-validation denial</b>: {@code checkCurrentUser} throws {@link ForbiddenException}
 *       because the user is not a metalake member. An {@link AuthorizationDenialFailureEvent} is
 *       dispatched, and the {@code operationFailureFired} flag again suppresses a duplicate.
 * </ul>
 */
@DisabledIfSystemProperty(named = ITUtils.TEST_MODE, matches = ITUtils.DEPLOY_TEST_MODE)
public class HttpAuditAuthzDenialIT extends BaseRestApiAuthorizationIT {

  private static final String LISTENER_NAME = "httpAuditAuthzCapture";

  /** A user who is authenticated but never added to {@link #METALAKE}. */
  private static final String NON_MEMBER_USER = "nonMemberUser";

  private static GravitinoAdminClient nonMemberClient;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Register the capturing listener before the parent starts the server
    customConfigs.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX
            + EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES,
        LISTENER_NAME);
    customConfigs.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX
            + LISTENER_NAME
            + "."
            + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        CapturedEventListener.class.getName());
    // Parent enables authorization, starts server, creates the metalake, and adds NORMAL_USER
    super.startIntegrationTest();

    // Create a client for a user who is NOT a metalake member
    nonMemberClient =
        GravitinoAdminClient.builder(serverUri).withSimpleAuth(NON_MEMBER_USER).build();
  }

  @AfterEach
  public void clearEvents() {
    CapturedEventListener.clear();
  }

  /**
   * When an authenticated metalake member lacking {@code CREATE_CATALOG} privilege calls {@code
   * createCatalog}, the authorization executor returns {@code false}. An {@link
   * AuthorizationDenialFailureEvent} is dispatched and the {@code operationFailureFired} flag is
   * set, so {@code HttpAuditFilter} skips emitting a duplicate failure event for the same request.
   *
   * <p>{@code listCatalogs} is intentionally not used here because it carries {@code expression =
   * ""} which bypasses the authorization executor entirely.
   */
  @Test
  public void testExecutorDenialOnCreateCatalogProducesEventAndNoDuplicate() {
    // NORMAL_USER is in the metalake but has no CREATE_CATALOG privilege
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            normalUserClient
                .loadMetalake(METALAKE)
                .createCatalog(
                    "denied-catalog", Catalog.Type.RELATIONAL, "hive", "", new HashMap<>()));
    awaitAtLeastOneEvent();

    List<Event> events = CapturedEventListener.getEvents();

    long authzDenialCount =
        events.stream().filter(e -> e instanceof AuthorizationDenialFailureEvent).count();
    long otherFailureCount =
        events.stream()
            .filter(e -> e instanceof FailureEvent)
            .filter(e -> !(e instanceof AuthorizationDenialFailureEvent))
            .count();

    Assertions.assertTrue(
        authzDenialCount >= 1,
        "Expected at least one AuthorizationDenialFailureEvent for a denied createCatalog");
    Assertions.assertEquals(
        0,
        otherFailureCount,
        "No duplicate failure event should be emitted when"
            + " AuthorizationDenialFailureEvent was already dispatched");

    AuthorizationDenialFailureEvent authzEvent =
        events.stream()
            .filter(e -> e instanceof AuthorizationDenialFailureEvent)
            .map(e -> (AuthorizationDenialFailureEvent) e)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No AuthorizationDenialFailureEvent found"));
    Assertions.assertEquals(NORMAL_USER, authzEvent.user());
    Assertions.assertEquals(OperationType.AUTHORIZATION_DENIAL, authzEvent.operationType());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, authzEvent.eventSource());
    // accessMetadataType = METALAKE for createCatalog → identifier is the metalake NameIdentifier
    Assertions.assertEquals(NameIdentifier.of(METALAKE), authzEvent.identifier());
    Map<String, String> customInfo = authzEvent.customInfo();
    Assertions.assertEquals("createCatalog", customInfo.get("auth.method"));
    Assertions.assertEquals(
        "METALAKE::CREATE_CATALOG || METALAKE::OWNER", customInfo.get("auth.expression"));
  }

  /**
   * When an authenticated metalake member calls {@code alterMetalake}, which requires {@code
   * METALAKE::OWNER}, the authorization executor returns {@code false} because {@link #NORMAL_USER}
   * was added as a plain member (not an owner). This is an executor denial for a different
   * operation and expression than {@link
   * #testExecutorDenialOnCreateCatalogProducesEventAndNoDuplicate()}.
   */
  @Test
  public void testExecutorDenialOnAlterMetalakeProducesCorrectEvent() {
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> normalUserClient.alterMetalake(METALAKE, MetalakeChange.updateComment("no-op")));
    awaitAtLeastOneEvent();

    List<Event> events = CapturedEventListener.getEvents();

    AuthorizationDenialFailureEvent authzEvent =
        events.stream()
            .filter(e -> e instanceof AuthorizationDenialFailureEvent)
            .map(e -> (AuthorizationDenialFailureEvent) e)
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "No AuthorizationDenialFailureEvent captured for alterMetalake"));
    Assertions.assertEquals(NORMAL_USER, authzEvent.user());
    Assertions.assertEquals(OperationType.AUTHORIZATION_DENIAL, authzEvent.operationType());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, authzEvent.eventSource());
    Assertions.assertEquals(NameIdentifier.of(METALAKE), authzEvent.identifier());
    Map<String, String> customInfo = authzEvent.customInfo();
    Assertions.assertEquals("alterMetalake", customInfo.get("auth.method"));
    Assertions.assertEquals("METALAKE::OWNER", customInfo.get("auth.expression"));
  }

  /**
   * When an authenticated user who is NOT a metalake member calls {@code loadMetalake}, {@code
   * checkCurrentUser} throws {@link ForbiddenException} before any operation proceeds. The
   * interceptor dispatches an {@link AuthorizationDenialFailureEvent} and sets the {@code
   * operationFailureFired} flag.
   *
   * <p>Note: {@link GravitinoAdminClient#loadMetalake} makes an immediate REST call, so {@code
   * loadMetalake} is the intercepted operation — the event carries {@code auth.method=loadMetalake}
   * and {@code auth.expression=METALAKE_USER}.
   */
  @Test
  public void testNonMemberOnLoadMetalakeProducesAuthorizationEvent() {
    // loadMetalake makes an immediate REST call; checkCurrentUser throws ForbiddenException for
    // non-members before the method body executes.
    Assertions.assertThrows(ForbiddenException.class, () -> nonMemberClient.loadMetalake(METALAKE));
    awaitAtLeastOneEvent();

    List<Event> events = CapturedEventListener.getEvents();

    Assertions.assertTrue(
        events.stream().anyMatch(e -> e instanceof AuthorizationDenialFailureEvent),
        "checkCurrentUser failure must dispatch AuthorizationDenialFailureEvent");

    AuthorizationDenialFailureEvent authzEvent =
        events.stream()
            .filter(e -> e instanceof AuthorizationDenialFailureEvent)
            .map(e -> (AuthorizationDenialFailureEvent) e)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No AuthorizationDenialFailureEvent found"));
    Assertions.assertEquals(NON_MEMBER_USER, authzEvent.user());
    Assertions.assertEquals(OperationType.AUTHORIZATION_DENIAL, authzEvent.operationType());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, authzEvent.eventSource());
    Assertions.assertEquals(NameIdentifier.of(METALAKE), authzEvent.identifier());
    Map<String, String> customInfo = authzEvent.customInfo();
    // loadMetalake uses
    // @AuthorizationExpression(expression = LOAD_METALAKE_AUTHORIZATION_EXPRESSION)
    Assertions.assertEquals("loadMetalake", customInfo.get("auth.method"));
    Assertions.assertEquals("METALAKE_USER", customInfo.get("auth.expression"));
  }

  /**
   * When a non-member calls {@code alterMetalake} (expression {@code METALAKE::OWNER}), {@code
   * checkCurrentUser} throws {@link ForbiddenException} before the expression executor runs. This
   * verifies that the user-validation denial also sets the {@code operationFailureFired} flag — no
   * duplicate {@link org.apache.gravitino.listener.api.event.server.HttpRequestFailureEvent} should
   * be emitted, even when the operation carries a non-empty authorization expression.
   */
  @Test
  public void testNonMemberOnAlterMetalakeProducesNoDuplicateFailureEvent() {
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> nonMemberClient.alterMetalake(METALAKE, MetalakeChange.updateComment("no-op")));
    awaitAtLeastOneEvent();

    List<Event> events = CapturedEventListener.getEvents();

    long authzDenialCount =
        events.stream().filter(e -> e instanceof AuthorizationDenialFailureEvent).count();
    long otherFailureCount =
        events.stream()
            .filter(e -> e instanceof FailureEvent)
            .filter(e -> !(e instanceof AuthorizationDenialFailureEvent))
            .count();

    Assertions.assertTrue(
        authzDenialCount >= 1,
        "Expected at least one AuthorizationDenialFailureEvent for non-member on alterMetalake");
    Assertions.assertEquals(
        0,
        otherFailureCount,
        "No duplicate failure event should be emitted when"
            + " AuthorizationDenialFailureEvent was already dispatched");

    AuthorizationDenialFailureEvent authzEvent =
        events.stream()
            .filter(e -> e instanceof AuthorizationDenialFailureEvent)
            .map(e -> (AuthorizationDenialFailureEvent) e)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No AuthorizationDenialFailureEvent found"));
    Assertions.assertEquals(NON_MEMBER_USER, authzEvent.user());
    Assertions.assertEquals(OperationType.AUTHORIZATION_DENIAL, authzEvent.operationType());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, authzEvent.eventSource());
    Assertions.assertEquals(NameIdentifier.of(METALAKE), authzEvent.identifier());
    Map<String, String> customInfo = authzEvent.customInfo();
    // checkCurrentUser fails before the expression is evaluated, but the annotation expression
    // is still recorded in auth.expression for auditability.
    Assertions.assertEquals("alterMetalake", customInfo.get("auth.method"));
    Assertions.assertEquals("METALAKE::OWNER", customInfo.get("auth.expression"));
  }

  // ─── Helper ──────────────────────────────────────────────────────────────────

  private void awaitAtLeastOneEvent() {
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> !CapturedEventListener.getEvents().isEmpty());
  }
}
