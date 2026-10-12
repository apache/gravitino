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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.CatalogEventDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;

/** Tests the lifecycle and credential isolation of catalog connection events. */
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
  void testFailureEventsDoNotExposeCredentials(ConnectionTestMode mode) throws Exception {
    Exception original = new Exception("jdbc-password=" + SECRET, new Exception("cause=" + SECRET));
    original.addSuppressed(new Exception("suppressed=" + SECRET));
    original.setStackTrace(
        new StackTraceElement[] {new StackTraceElement(SECRET, SECRET, SECRET, 1)});
    invoke(mode, doThrow(original).when(delegate));

    Assertions.assertSame(
        original, Assertions.assertThrowsExactly(Exception.class, () -> invokeAsUser(mode)));

    TestConnectionFailureEvent event =
        (TestConnectionFailureEvent)
            assertEvents(mode, TestConnectionFailureEvent.class, OperationStatus.FAILURE);
    Exception sanitized = event.exception();
    Assertions.assertNotSame(original, sanitized);
    Assertions.assertEquals(
        "Catalog connection test failed: java.lang.Exception", sanitized.getMessage());
    Assertions.assertNull(sanitized.getCause());
    Assertions.assertEquals(0, sanitized.getSuppressed().length);
    StringWriter stackTrace = new StringWriter();
    try (PrintWriter writer = new PrintWriter(stackTrace)) {
      sanitized.printStackTrace(writer);
    }
    Assertions.assertFalse(stackTrace.toString().contains(SECRET));
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

  private Event assertEvents(
      ConnectionTestMode mode, Class<? extends Event> eventClass, OperationStatus status)
      throws Exception {
    Assertions.assertEquals(1, listener.getPreEvents().size());
    Assertions.assertEquals(1, listener.getPostEvents().size());
    PreEvent preEvent = listener.popPreEvent();
    Assertions.assertInstanceOf(TestConnectionPreEvent.class, preEvent);
    assertEventFields(preEvent, OperationStatus.UNPROCESSED);
    Event event = listener.popPostEvent();
    Assertions.assertEquals(eventClass, event.getClass());
    assertEventFields(event, status);

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
