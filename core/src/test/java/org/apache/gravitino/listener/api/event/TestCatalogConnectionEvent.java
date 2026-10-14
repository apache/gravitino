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

package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.audit.JsonAuditFormatter;
import org.apache.gravitino.audit.v2.SimpleFormatterV2;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.CatalogEventDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.info.CatalogInfo;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;

/** Tests the lifecycle, request context, and audit output of catalog connection events. */
public class TestCatalogConnectionEvent {
  private static final NameIdentifier IDENTIFIER = NameIdentifier.of("metalake", "catalog");
  private static final String USER = "connection-test-user";
  private static final String SECRET = "connection-test-secret";
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("jdbc-password", SECRET);
  private static final CatalogChange[] CHANGES =
      new CatalogChange[] {CatalogChange.setProperty("jdbc-password", SECRET)};

  private DummyEventListener listener;
  private CatalogDispatcher delegate;
  private CatalogEventDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    listener = spy(new DummyEventListener());
    delegate = mock(CatalogDispatcher.class);
    dispatcher =
        new CatalogEventDispatcher(new EventBus(Collections.singletonList(listener)), delegate);
  }

  @ParameterizedTest
  @EnumSource(ConnectionTestMode.class)
  void testSuccessEvents(ConnectionTestMode mode) throws Exception {
    invokeAsUser(mode);

    assertEvents(mode, TestConnectionEvent.class, OperationStatus.SUCCESS);
  }

  @ParameterizedTest
  @EnumSource(ConnectionTestMode.class)
  void testFailureEventsPreserveOriginalException(ConnectionTestMode mode) throws Exception {
    Exception original = new Exception("jdbc-password=" + SECRET, new Exception("cause=" + SECRET));
    original.addSuppressed(new Exception("suppressed=" + SECRET));
    invoke(mode, doThrow(original).when(delegate));

    Assertions.assertSame(
        original, Assertions.assertThrowsExactly(Exception.class, () -> invokeAsUser(mode)));

    TestConnectionFailureEvent event =
        (TestConnectionFailureEvent)
            assertEvents(mode, TestConnectionFailureEvent.class, OperationStatus.FAILURE);
    Assertions.assertSame(original, event.exception());
  }

  @ParameterizedTest
  @EnumSource(ConnectionTestMode.class)
  void testPreEventVetoPreventsConnectionTest(ConnectionTestMode mode) {
    ForbiddenException forbidden = new ForbiddenException("denied");
    doThrow(forbidden).when(listener).onPreEvent(any(TestConnectionPreEvent.class));

    Assertions.assertSame(
        forbidden,
        Assertions.assertThrowsExactly(ForbiddenException.class, () -> invokeAsUser(mode)));

    verify(listener).onPreEvent(any(TestConnectionPreEvent.class));
    verify(listener, never()).onPostEvent(any(Event.class));
    verifyNoInteractions(delegate);
  }

  @ParameterizedTest
  @EnumSource(ConnectionTestMode.class)
  void testFailureListenerDoesNotMaskOriginalException(ConnectionTestMode mode) throws Exception {
    Exception original = new Exception("jdbc-password=" + SECRET);
    invoke(mode, doThrow(original).when(delegate));
    doThrow(new IllegalStateException("listener failed"))
        .when(listener)
        .onPostEvent(any(TestConnectionFailureEvent.class));

    Assertions.assertSame(
        original, Assertions.assertThrowsExactly(Exception.class, () -> invokeAsUser(mode)));

    verify(listener).onPreEvent(any(TestConnectionPreEvent.class));
    verify(listener).onPostEvent(any(TestConnectionFailureEvent.class));
    verify(listener, never()).onPostEvent(any(TestConnectionEvent.class));
    invoke(mode, verify(delegate));
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void testIdentifierOnlyConstructors() {
    TestConnectionPreEvent preEvent = new TestConnectionPreEvent(USER, IDENTIFIER);
    assertRequestContext(
        ConnectionTestMode.STORED, preEvent.catalogInfo(), preEvent.catalogChanges());
    TestConnectionEvent event = new TestConnectionEvent(USER, IDENTIFIER);
    assertRequestContext(ConnectionTestMode.STORED, event.catalogInfo(), event.catalogChanges());
    Exception original = new Exception("connection failed");
    TestConnectionFailureEvent failureEvent =
        new TestConnectionFailureEvent(USER, IDENTIFIER, original);
    assertRequestContext(
        ConnectionTestMode.STORED, failureEvent.catalogInfo(), failureEvent.catalogChanges());
    Assertions.assertSame(original, failureEvent.exception());
  }

  private Event assertEvents(
      ConnectionTestMode mode, Class<? extends Event> eventClass, OperationStatus status)
      throws Exception {
    Assertions.assertEquals(1, listener.getPreEvents().size());
    Assertions.assertEquals(1, listener.getPostEvents().size());
    TestConnectionPreEvent preEvent =
        Assertions.assertInstanceOf(TestConnectionPreEvent.class, listener.popPreEvent());
    assertEventFields(preEvent, OperationStatus.UNPROCESSED);
    assertRequestContext(mode, preEvent.catalogInfo(), preEvent.catalogChanges());
    Event event = listener.popPostEvent();
    Assertions.assertEquals(eventClass, event.getClass());
    assertEventFields(event, status);
    if (event instanceof TestConnectionEvent) {
      TestConnectionEvent successEvent = (TestConnectionEvent) event;
      assertRequestContext(mode, successEvent.catalogInfo(), successEvent.catalogChanges());
    } else {
      TestConnectionFailureEvent failureEvent =
          Assertions.assertInstanceOf(TestConnectionFailureEvent.class, event);
      assertRequestContext(mode, failureEvent.catalogInfo(), failureEvent.catalogChanges());
    }
    assertAuditOutput(new JsonAuditFormatter().format(event).toString());
    assertAuditOutput(new SimpleFormatterV2().format(event).toString());

    InOrder ordered = inOrder(listener, delegate);
    ordered.verify(listener).onPreEvent(preEvent);
    invoke(mode, ordered.verify(delegate));
    ordered.verify(listener).onPostEvent(event);
    verifyNoMoreInteractions(delegate);
    return event;
  }

  private void assertEventFields(BaseEvent event, OperationStatus status) {
    Assertions.assertEquals(USER, event.user());
    Assertions.assertEquals(IDENTIFIER, event.identifier());
    Assertions.assertEquals(OperationType.TEST_CONNECTION_CATALOG, event.operationType());
    Assertions.assertEquals(status, event.operationStatus());
    Assertions.assertTrue(event.customInfo().isEmpty());
  }

  private void assertRequestContext(
      ConnectionTestMode mode,
      @Nullable CatalogInfo catalogInfo,
      @Nullable CatalogChange[] catalogChanges) {
    if (mode == ConnectionTestMode.PRE_CREATE) {
      Assertions.assertNotNull(catalogInfo);
      Assertions.assertEquals(IDENTIFIER.name(), catalogInfo.name());
      Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogInfo.type());
      Assertions.assertEquals("jdbc-mysql", catalogInfo.provider());
      Assertions.assertEquals("comment", catalogInfo.comment());
      Assertions.assertEquals(PROPERTIES, catalogInfo.properties());
      Assertions.assertNull(catalogInfo.auditInfo());
    } else {
      Assertions.assertNull(catalogInfo);
    }

    if (mode == ConnectionTestMode.PROPOSED_CHANGES) {
      Assertions.assertArrayEquals(CHANGES, catalogChanges);
      Assertions.assertNotSame(CHANGES, catalogChanges);
    } else if (mode == ConnectionTestMode.EMPTY_CHANGES) {
      Assertions.assertArrayEquals(new CatalogChange[0], catalogChanges);
    } else {
      Assertions.assertNull(catalogChanges);
    }
  }

  private void assertAuditOutput(String auditLog) {
    Assertions.assertTrue(auditLog.contains(USER));
    Assertions.assertTrue(auditLog.contains(IDENTIFIER.toString()));
    Assertions.assertTrue(auditLog.contains(OperationType.TEST_CONNECTION_CATALOG.name()));
    Assertions.assertFalse(auditLog.contains(SECRET));
    Assertions.assertFalse(auditLog.contains("jdbc-password"));
  }

  private void invokeAsUser(ConnectionTestMode mode) throws Exception {
    PrincipalUtils.doAs(
        new UserPrincipal(USER),
        () -> {
          invoke(mode, dispatcher);
          return null;
        });
  }

  private void invoke(ConnectionTestMode mode, CatalogDispatcher target) throws Exception {
    switch (mode) {
      case PRE_CREATE:
        target.testConnection(
            IDENTIFIER, Catalog.Type.RELATIONAL, "jdbc-mysql", "comment", PROPERTIES);
        break;
      case STORED:
        target.testConnection(IDENTIFIER);
        break;
      case PROPOSED_CHANGES:
        target.testConnection(IDENTIFIER, CHANGES);
        break;
      case EMPTY_CHANGES:
        target.testConnection(IDENTIFIER, new CatalogChange[0]);
        break;
      default:
        throw new AssertionError("Unexpected connection test mode: " + mode);
    }
  }

  private enum ConnectionTestMode {
    PRE_CREATE,
    STORED,
    PROPOSED_CHANGES,
    EMPTY_CHANGES
  }
}
