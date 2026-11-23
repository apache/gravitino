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
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TestDeferredConnectorMetadata {
  @Test
  void managementLifecycleDoesNotAuthenticate() {
    AtomicInteger calls = new AtomicInteger();
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(
            mock(ConnectorSession.class),
            currentSession -> {
              calls.incrementAndGet();
              throw new IllegalStateException("No user token in management session");
            });
    ConnectorSession session = mock(ConnectorSession.class);
    metadata.beginQuery(session);
    assertEquals("DeferredConnectorMetadata", metadata.toString());
    assertEquals(metadata, metadata);
    metadata.hashCode();
    metadata.cleanupQuery(session);
    assertEquals(0, calls.get());
    assertThrows(IllegalStateException.class, () -> metadata.listSchemaNames(session));
    assertEquals(0, calls.get());
  }

  @Test
  void dataOperationInitializesOnceAndPreservesLifecycleOrder() {
    ConnectorMetadata nativeMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    when(nativeMetadata.listSchemaNames(session)).thenReturn(List.of("demo"));
    AtomicInteger calls = new AtomicInteger();
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(
            mock(ConnectorSession.class),
            currentSession -> {
              calls.incrementAndGet();
              return nativeMetadata;
            });
    metadata.beginQuery(session);
    verifyNoInteractions(nativeMetadata);
    assertEquals(List.of("demo"), metadata.listSchemaNames(session));
    assertEquals(List.of("demo"), metadata.listSchemaNames(session));
    metadata.cleanupQuery(session);
    assertEquals(1, calls.get());
    var order = inOrder(nativeMetadata);
    order.verify(nativeMetadata).beginQuery(session);
    order.verify(nativeMetadata, times(2)).listSchemaNames(session);
    order.verify(nativeMetadata).cleanupQuery(session);
  }

  @Test
  void initializedMetadataSupportsTwoQueryLifecycles() {
    ConnectorMetadata nativeMetadata = mock(ConnectorMetadata.class);
    ConnectorSession first = mock(ConnectorSession.class);
    ConnectorSession second = mock(ConnectorSession.class);
    AtomicInteger calls = new AtomicInteger();
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(
            first,
            currentSession -> {
              calls.incrementAndGet();
              return nativeMetadata;
            });
    metadata.beginQuery(first);
    metadata.listSchemaNames(first);
    metadata.cleanupQuery(first);
    assertThrows(IllegalStateException.class, () -> metadata.listSchemaNames(second));
    metadata.beginQuery(second);
    metadata.listSchemaNames(second);
    metadata.cleanupQuery(second);
    assertEquals(1, calls.get());
    var order = inOrder(nativeMetadata);
    order.verify(nativeMetadata).beginQuery(first);
    order.verify(nativeMetadata).listSchemaNames(first);
    order.verify(nativeMetadata).cleanupQuery(first);
    order.verify(nativeMetadata).beginQuery(second);
    order.verify(nativeMetadata).listSchemaNames(second);
    order.verify(nativeMetadata).cleanupQuery(second);
    order.verifyNoMoreInteractions();
  }

  @Test
  void managementQueryCanBeFollowedByDeferredDataQuery() {
    ConnectorSession management = mock(ConnectorSession.class);
    ConnectorSession user = mock(ConnectorSession.class);
    ConnectorMetadata nativeMetadata = mock(ConnectorMetadata.class);
    AtomicInteger calls = new AtomicInteger();
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(
            management,
            currentSession -> {
              calls.incrementAndGet();
              assertSame(user, currentSession);
              return nativeMetadata;
            });
    metadata.beginQuery(management);
    metadata.cleanupQuery(management);
    assertThrows(IllegalStateException.class, () -> metadata.listSchemaNames(user));
    assertEquals(0, calls.get());
    metadata.beginQuery(user);
    assertEquals(0, calls.get());
    metadata.listSchemaNames(user);
    metadata.cleanupQuery(user);
    assertEquals(1, calls.get());
    var order = inOrder(nativeMetadata);
    order.verify(nativeMetadata).beginQuery(user);
    order.verify(nativeMetadata).listSchemaNames(user);
    order.verify(nativeMetadata).cleanupQuery(user);
    order.verifyNoMoreInteractions();
  }

  @Test
  void failedBeginQueryStillCleansUpNativeMetadata() {
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorMetadata nativeMetadata = mock(ConnectorMetadata.class);
    IllegalStateException failure = new IllegalStateException("Query initialization failed");
    doThrow(failure).when(nativeMetadata).beginQuery(session);
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(session, currentSession -> nativeMetadata);
    metadata.beginQuery(session);
    assertSame(
        failure,
        assertThrows(IllegalStateException.class, () -> metadata.listSchemaNames(session)));
    metadata.cleanupQuery(session);
    var order = inOrder(nativeMetadata);
    order.verify(nativeMetadata).beginQuery(session);
    order.verify(nativeMetadata).cleanupQuery(session);
    order.verifyNoMoreInteractions();
  }

  @Test
  void authenticationFailurePropagatesWithoutFallback() {
    IllegalArgumentException failure = new IllegalArgumentException("Token rejected");
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(
            mock(ConnectorSession.class),
            currentSession -> {
              throw failure;
            });
    ConnectorSession session = mock(ConnectorSession.class);
    metadata.beginQuery(session);
    assertSame(
        failure,
        assertThrows(IllegalArgumentException.class, () -> metadata.listSchemaNames(session)));
    assertDoesNotThrow(() -> metadata.cleanupQuery(session));
  }

  @Test
  void permissionFailureIsNotWrappedByReflection() {
    ConnectorMetadata nativeMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    SecurityException failure = new SecurityException("Access denied");
    when(nativeMetadata.listSchemaNames(session)).thenThrow(failure);
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(
            mock(ConnectorSession.class), currentSession -> nativeMetadata);
    assertSame(
        failure, assertThrows(SecurityException.class, () -> metadata.listSchemaNames(session)));
  }

  @Test
  void differentQueriesDoNotShareNativeMetadata() {
    ConnectorMetadata alice = mock(ConnectorMetadata.class);
    ConnectorMetadata bob = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorMetadata a =
        DeferredConnectorMetadata.create(mock(ConnectorSession.class), currentSession -> alice);
    ConnectorMetadata b =
        DeferredConnectorMetadata.create(mock(ConnectorSession.class), currentSession -> bob);
    a.listSchemaNames(session);
    verifyNoInteractions(bob);
    b.listSchemaNames(session);
    verify(alice).listSchemaNames(session);
    verify(bob).listSchemaNames(session);
  }

  @Test
  void initializationUsesOperationSessionInsteadOfCreationSession() {
    ConnectorSession management = mock(ConnectorSession.class);
    ConnectorSession user = mock(ConnectorSession.class);
    ConnectorMetadata nativeMetadata = mock(ConnectorMetadata.class);
    ConnectorMetadata metadata =
        DeferredConnectorMetadata.create(
            management,
            currentSession -> {
              assertSame(user, currentSession);
              return nativeMetadata;
            });
    metadata.beginQuery(management);
    metadata.listSchemaNames(user);
    verify(nativeMetadata).beginQuery(user);
    verify(nativeMetadata).listSchemaNames(user);
  }
}
