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

package org.apache.gravitino.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.CreateTableEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Covers the audit-extras path on {@link TableEventDispatcher}: how facts stashed on {@link
 * RequestContext} reach the emitted event, and how listener failures are contained once an
 * operation opts in. Event field mapping itself is covered by {@code TestTableEvent}.
 */
public class TestTableEventDispatcher {

  private static final NameIdentifier IDENT =
      NameIdentifier.of("metalake", "catalog", "schema", "table");

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
    doThrow(new RuntimeException("listener failed")).when(listener).onPostEvent(any(Event.class));

    RuntimeException withoutExtras =
        Assertions.assertThrows(
            RuntimeException.class, () -> createTable(dispatcher(listener, mockTable())));
    Assertions.assertEquals("listener failed", withoutExtras.getMessage());

    RequestContext.setAuditExtras(ImmutableMap.of("audit.reason", "policy-applied"));
    RuntimeException withExtras =
        Assertions.assertThrows(
            RuntimeException.class, () -> createTable(dispatcher(listener, mockTable())));
    Assertions.assertEquals("listener failed", withExtras.getMessage());
  }

  /**
   * The point of routing extras through the request context is to enrich the event the dispatcher
   * already emits rather than to publish a second one. Pins both halves: the fact reaches {@code
   * customInfo}, and exactly one post event is produced for the operation.
   */
  @Test
  void testCreateWithExtrasDispatchesOneCreateTableEvent() {
    DummyEventListener listener = new DummyEventListener();
    TableEventDispatcher dispatcher = dispatcher(listener, mockTable());
    RequestContext.setAuditExtras(ImmutableMap.of("audit.reason", "policy-applied"));

    createTable(dispatcher);

    Event event = listener.popPostEvent();
    Assertions.assertEquals(CreateTableEvent.class, event.getClass());
    Assertions.assertEquals("policy-applied", event.customInfo().get("audit.reason"));
    Assertions.assertTrue(listener.getPostEvents().isEmpty());
  }

  /**
   * Server threads are pooled, so a fact left behind by one request would be mis-attributed to
   * whichever request reuses the thread next. Pins that the dispatcher consumes the stash rather
   * than reading it, by running a second operation on the same thread and requiring clean extras.
   */
  @Test
  void testExtrasDoNotLeakIntoTheNextOperationOnTheSameThread() {
    DummyEventListener listener = new DummyEventListener();
    TableEventDispatcher dispatcher = dispatcher(listener, mockTable());
    RequestContext.setAuditExtras(ImmutableMap.of("audit.reason", "policy-applied"));

    createTable(dispatcher);
    Assertions.assertEquals(
        "policy-applied", listener.popPostEvent().customInfo().get("audit.reason"));

    createTable(dispatcher);

    Event second = listener.popPostEvent();
    Assertions.assertEquals(CreateTableEvent.class, second.getClass());
    Assertions.assertTrue(
        second.customInfo().isEmpty(), "Extras must not survive into a later operation");
  }

  private static TableEventDispatcher dispatcher(EventListenerPlugin listener, Table table) {
    TableDispatcher catalog = mock(TableDispatcher.class);
    when(catalog.createTable(
            any(NameIdentifier.class),
            any(Column[].class),
            any(String.class),
            any(Map.class),
            any(Transform[].class),
            any(Distribution.class),
            any(SortOrder[].class),
            any(Index[].class)))
        .thenReturn(table);
    return new TableEventDispatcher(new EventBus(Collections.singletonList(listener)), catalog);
  }

  private static void createTable(TableEventDispatcher dispatcher) {
    dispatcher.createTable(
        IDENT,
        new Column[] {Column.of("id", Types.LongType.get())},
        "comment",
        ImmutableMap.of(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        Indexes.EMPTY_INDEXES);
  }

  private static Table mockTable() {
    Table table = mock(Table.class);
    when(table.name()).thenReturn("table");
    when(table.comment()).thenReturn("comment");
    when(table.columns()).thenReturn(new Column[] {Column.of("id", Types.LongType.get())});
    when(table.properties()).thenReturn(ImmutableMap.of());
    when(table.partitioning()).thenReturn(new Transform[0]);
    when(table.distribution()).thenReturn(Distributions.NONE);
    when(table.sortOrder()).thenReturn(new SortOrder[0]);
    when(table.index()).thenReturn(Indexes.EMPTY_INDEXES);
    when(table.auditInfo()).thenReturn(null);
    return table;
  }
}
