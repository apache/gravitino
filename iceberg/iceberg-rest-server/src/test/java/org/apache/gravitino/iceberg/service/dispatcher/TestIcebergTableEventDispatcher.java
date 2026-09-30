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

package org.apache.gravitino.iceberg.service.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.IcebergCreateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergEvent;
import org.apache.gravitino.listener.api.event.IcebergFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTableEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableFailureEvent;
import org.apache.gravitino.utils.RequestContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Covers the audit-extras path on {@link IcebergTableEventDispatcher}: how facts stashed on {@link
 * RequestContext} reach the emitted Iceberg event, and that listener-failure propagation is
 * identical with or without extras. Event field mapping itself is covered by {@code
 * TestIcebergRequestContext}.
 */
public class TestIcebergTableEventDispatcher {

  private static final String METALAKE = "metalake";
  private static final String CATALOG = "catalog";
  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "table");
  private static final Schema TABLE_SCHEMA =
      new Schema(NestedField.required(1, "id", LongType.get()));
  private static final String REQUEST_HEADER = "X-Request-Id";
  private static final String REQUEST_HEADER_VALUE = "req-1";
  private static final String EXTRA_KEY = "audit.reason";

  @AfterEach
  void cleanup() {
    RequestContext.clear();
    Thread.interrupted();
  }

  /**
   * This change is meant to be purely additive: extras enrich the event, and nothing about error
   * propagation moves. A synchronous listener failure therefore has to reach the caller exactly as
   * it does today, whether or not extras were stashed. Asserting both halves in one test is what
   * makes a future swallow branch visible, since adding one would only break the extras case.
   */
  @Test
  void testAttachingExtrasDoesNotChangeListenerFailurePropagation() {
    EventListenerPlugin listener = mock(EventListenerPlugin.class);
    when(listener.transformPreEvent(any())).thenAnswer(invocation -> invocation.getArgument(0));
    doThrow(new RuntimeException("listener failed")).when(listener).onPostEvent(any(Event.class));

    IcebergTableEventDispatcher dispatcher = dispatcher(listener, succeedingInner());

    RuntimeException withoutExtras =
        Assertions.assertThrows(RuntimeException.class, () -> createTable(dispatcher));
    Assertions.assertEquals("listener failed", withoutExtras.getMessage());

    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "policy-applied"));
    RuntimeException withExtras =
        Assertions.assertThrows(RuntimeException.class, () -> createTable(dispatcher));
    Assertions.assertEquals("listener failed", withExtras.getMessage());
  }

  /**
   * The point of routing extras through the request context is to enrich the event the dispatcher
   * already emits rather than to publish a second one. Pins both halves: headers ∪ extras reach
   * {@code customInfo}, extras stay off {@code httpHeaders}, and exactly one post event is
   * produced.
   */
  @Test
  void testCreateWithExtrasMergesHeadersAndDoesNotTouchHttpHeaders() {
    RecordingListener listener = new RecordingListener();
    IcebergTableEventDispatcher dispatcher = dispatcher(listener, succeedingInner());
    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "policy-applied"));

    createTable(dispatcher);

    Event event = listener.popPostEvent();
    Assertions.assertEquals(IcebergCreateTableEvent.class, event.getClass());
    assertHeadersMergedWithExtras(event, "policy-applied");
    Assertions.assertTrue(listener.postEvents.isEmpty());
  }

  @Test
  void testCreateWithoutExtrasKeepsHeaderOnlyCustomInfo() {
    RecordingListener listener = new RecordingListener();
    IcebergTableEventDispatcher dispatcher = dispatcher(listener, succeedingInner());

    IcebergRequestContext context = requestContext();
    dispatcher.createTable(context, NAMESPACE, createTableRequest());

    Event event = listener.popPostEvent();
    Assertions.assertEquals(IcebergCreateTableEvent.class, event.getClass());
    // Content equality, not identity: Event.customInfo() now merges in the request's
    // automatically captured query parameters, so the result is always a freshly built map even
    // when that automatic contribution is empty.
    Assertions.assertEquals(context.httpHeaders(), event.customInfo());
    Assertions.assertFalse(event.customInfo().containsKey(EXTRA_KEY));
  }

  /**
   * {@code IcebergEvent}/{@code IcebergFailureEvent} used to override {@code customInfo()} to
   * return only the request-context's own facts (headers ∪ extras), silently discarding {@link
   * Event}'s automatically captured query parameters. Pins that the two are now merged, for both
   * the success and failure event.
   */
  @Test
  void testCustomInfoMergesAutomaticQueryParamsWithRequestContextFacts() {
    RecordingListener listener = new RecordingListener();
    IcebergTableEventDispatcher dispatcher = dispatcher(listener, succeedingInner());
    RequestContext.setRequestQueryParams(ImmutableMap.of("details", "true"));

    createTable(dispatcher);

    Event event = listener.popPostEvent();
    Assertions.assertEquals(IcebergCreateTableEvent.class, event.getClass());
    Assertions.assertEquals("true", event.customInfo().get("details"));
    Assertions.assertEquals(REQUEST_HEADER_VALUE, event.customInfo().get(REQUEST_HEADER));
  }

  /**
   * A contributor that rejected the operation is exactly the case where the reason matters most, so
   * extras have to survive the exception path and reach the failure event.
   */
  @Test
  void testCreateFailureEventMergesHeadersAndExtras() {
    RecordingListener listener = new RecordingListener();
    IcebergTableOperationDispatcher inner = mock(IcebergTableOperationDispatcher.class);
    when(inner.createTable(any(), any(), any())).thenThrow(new RuntimeException("create failed"));
    IcebergTableEventDispatcher dispatcher = dispatcher(listener, inner);
    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "validation-failed"));

    RuntimeException thrown =
        Assertions.assertThrows(RuntimeException.class, () -> createTable(dispatcher));
    Assertions.assertEquals("create failed", thrown.getMessage());

    Event event = listener.popPostEvent();
    Assertions.assertEquals(IcebergCreateTableFailureEvent.class, event.getClass());
    assertHeadersMergedWithExtras(event, "validation-failed");
  }

  /**
   * Each Iceberg table operation reads the stash in its own hand-written branch, so create passing
   * does not imply update and load pass. Covers the two remaining operations and, by stashing a
   * second fact between them, that consecutive operations on one thread get their own value.
   */
  @Test
  void testUpdateAndLoadEventsAttachStashedExtras() {
    RecordingListener listener = new RecordingListener();
    IcebergTableEventDispatcher dispatcher = dispatcher(listener, succeedingInner());

    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "policy-applied"));
    dispatcher.updateTable(requestContext(), TABLE_ID, updateTableRequest());
    Event updateEvent = listener.popPostEvent();
    Assertions.assertEquals(IcebergUpdateTableEvent.class, updateEvent.getClass());
    assertHeadersMergedWithExtras(updateEvent, "policy-applied");

    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "cache-miss"));
    dispatcher.loadTable(requestContext(), TABLE_ID);
    Event loadEvent = listener.popPostEvent();
    Assertions.assertEquals(IcebergLoadTableEvent.class, loadEvent.getClass());
    assertHeadersMergedWithExtras(loadEvent, "cache-miss");
  }

  @Test
  void testUpdateAndLoadFailureEventsAttachStashedExtras() {
    RecordingListener listener = new RecordingListener();
    IcebergTableOperationDispatcher inner = mock(IcebergTableOperationDispatcher.class);
    when(inner.updateTable(any(), any(), any())).thenThrow(new RuntimeException("update failed"));
    when(inner.loadTable(any(), any())).thenThrow(new RuntimeException("load failed"));
    IcebergTableEventDispatcher dispatcher = dispatcher(listener, inner);

    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "validation-failed"));
    RuntimeException updateThrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> dispatcher.updateTable(requestContext(), TABLE_ID, updateTableRequest()));
    Assertions.assertEquals("update failed", updateThrown.getMessage());
    Event updateEvent = listener.popPostEvent();
    Assertions.assertEquals(IcebergUpdateTableFailureEvent.class, updateEvent.getClass());
    assertHeadersMergedWithExtras(updateEvent, "validation-failed");

    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "not-found"));
    RuntimeException loadThrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> dispatcher.loadTable(requestContext(), TABLE_ID));
    Assertions.assertEquals("load failed", loadThrown.getMessage());
    Event loadEvent = listener.popPostEvent();
    Assertions.assertEquals(IcebergLoadTableFailureEvent.class, loadEvent.getClass());
    assertHeadersMergedWithExtras(loadEvent, "not-found");
  }

  /**
   * Server threads are pooled, so a fact left behind by one request would be mis-attributed to
   * whichever request reuses the thread next. Pins that the dispatcher consumes the stash rather
   * than reading it, by running a second operation on the same thread and requiring clean extras.
   */
  @Test
  void testExtrasDoNotLeakIntoTheNextOperationOnTheSameThread() {
    RecordingListener listener = new RecordingListener();
    IcebergTableEventDispatcher dispatcher = dispatcher(listener, succeedingInner());
    RequestContext.setAuditExtras(ImmutableMap.of(EXTRA_KEY, "policy-applied"));

    createTable(dispatcher);
    Assertions.assertEquals("policy-applied", listener.popPostEvent().customInfo().get(EXTRA_KEY));

    createTable(dispatcher);

    Event second = listener.popPostEvent();
    Assertions.assertEquals(IcebergCreateTableEvent.class, second.getClass());
    Assertions.assertFalse(
        second.customInfo().containsKey(EXTRA_KEY),
        "Extras must not survive into a later operation");
    // Content equality, not identity: Event.customInfo() now merges in the request's
    // automatically captured query parameters, so the result is always a freshly built map even
    // when that automatic contribution is empty.
    Assertions.assertEquals(
        ((IcebergEvent) second).icebergRequestContext().httpHeaders(), second.customInfo());
  }

  private static void assertHeadersMergedWithExtras(Event event, String extraValue) {
    Assertions.assertEquals(REQUEST_HEADER_VALUE, event.customInfo().get(REQUEST_HEADER));
    Assertions.assertEquals(extraValue, event.customInfo().get(EXTRA_KEY));
    Map<String, String> headers;
    if (event instanceof IcebergEvent) {
      headers = ((IcebergEvent) event).icebergRequestContext().httpHeaders();
    } else {
      headers = ((IcebergFailureEvent) event).icebergRequestContext().httpHeaders();
    }
    Assertions.assertFalse(headers.containsKey(EXTRA_KEY));
    Assertions.assertEquals(REQUEST_HEADER_VALUE, headers.get(REQUEST_HEADER));
  }

  private static IcebergTableEventDispatcher dispatcher(
      EventListenerPlugin listener, IcebergTableOperationDispatcher inner) {
    return new IcebergTableEventDispatcher(
        inner, new EventBus(Collections.singletonList(listener)), METALAKE);
  }

  private static IcebergTableOperationDispatcher succeedingInner() {
    IcebergTableOperationDispatcher inner = mock(IcebergTableOperationDispatcher.class);
    LoadTableResponse response = loadTableResponse();
    when(inner.createTable(any(), any(), any())).thenReturn(response);
    when(inner.updateTable(any(), any(), any())).thenReturn(response);
    when(inner.loadTable(any(), any())).thenReturn(response);
    return inner;
  }

  private static void createTable(IcebergTableEventDispatcher dispatcher) {
    dispatcher.createTable(requestContext(), NAMESPACE, createTableRequest());
  }

  private static IcebergRequestContext requestContext() {
    return new IcebergRequestContext(
        requestWithHeader(REQUEST_HEADER, REQUEST_HEADER_VALUE), CATALOG);
  }

  private static CreateTableRequest createTableRequest() {
    return CreateTableRequest.builder().withName(TABLE_ID.name()).withSchema(TABLE_SCHEMA).build();
  }

  private static UpdateTableRequest updateTableRequest() {
    return new UpdateTableRequest(Collections.emptyList(), Collections.emptyList());
  }

  private static LoadTableResponse loadTableResponse() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            TABLE_SCHEMA,
            PartitionSpec.unpartitioned(),
            "file:///tmp/iceberg-audit-extras",
            ImmutableMap.of());
    return LoadTableResponse.builder().withTableMetadata(metadata).build();
  }

  private static HttpServletRequest requestWithHeader(String name, String value) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    Enumeration<String> headerNames = Collections.enumeration(Collections.singleton(name));
    when(request.getRemoteHost()).thenReturn("localhost");
    when(request.getHeaderNames()).thenReturn(headerNames);
    when(request.getHeader(name)).thenReturn(value);
    return request;
  }

  private static final class RecordingListener implements EventListenerPlugin {
    private final LinkedList<Event> postEvents = new LinkedList<>();

    @Override
    public void init(Map<String, String> properties) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void onPostEvent(Event event) {
      postEvents.add(event);
    }

    Event popPostEvent() {
      Assertions.assertFalse(postEvents.isEmpty(), "No post events to pop");
      return postEvents.removeLast();
    }
  }
}
