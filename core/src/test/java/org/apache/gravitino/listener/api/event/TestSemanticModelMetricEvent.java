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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SemanticModelEventDispatcher;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelMetricEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelMetricFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelMetricInfosEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelMetricInfosFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelMetricInfosPreEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelMetricPreEvent;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class TestSemanticModelMetricEvent {
  private static final Namespace NAMESPACE = Namespace.of("lake", "catalog", "schema");

  @ParameterizedTest
  @CsvSource({"false,-1", "false,0", "false,2", "true,-1", "true,0", "true,2"})
  void testSuccessEvents(boolean details, int count) {
    EventBus bus = mock(EventBus.class);
    SemanticModelDispatcher delegate = mock(SemanticModelDispatcher.class);
    SemanticModelEventDispatcher dispatcher = new SemanticModelEventDispatcher(bus, delegate);
    NameIdentifier[] names = count < 0 ? null : new NameIdentifier[count];
    SemanticModelMetric[] infos = count < 0 ? null : new SemanticModelMetric[count];
    when(delegate.listMetrics(NAMESPACE)).thenReturn(names);
    when(delegate.listMetricInfos(NAMESPACE)).thenReturn(infos);
    try (MockedStatic<PrincipalUtils> principal = mockStatic(PrincipalUtils.class)) {
      principal.when(PrincipalUtils::getCurrentUserName).thenReturn("initiator", "changed");
      if (details) assertSame(infos, dispatcher.listMetricInfos(NAMESPACE));
      else assertSame(names, dispatcher.listMetrics(NAMESPACE));
      principal.verify(PrincipalUtils::getCurrentUserName);
    }
    List<BaseEvent> events = events(bus, details);
    assertEquals(
        details ? ListSemanticModelMetricInfosEvent.class : ListSemanticModelMetricEvent.class,
        events.get(1).getClass());
    assertEquals(count, ((ListEvent) events.get(1)).resultCount());
    assertEquals(OperationStatus.SUCCESS, ((Event) events.get(1)).operationStatus());
    assertEquals(
        NAMESPACE,
        details
            ? ((ListSemanticModelMetricInfosEvent) events.get(1)).namespace()
            : ((ListSemanticModelMetricEvent) events.get(1)).namespace());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testFailuresAreReportedAndRethrown(boolean details) {
    EventBus bus = mock(EventBus.class);
    SemanticModelDispatcher delegate = mock(SemanticModelDispatcher.class);
    SemanticModelEventDispatcher dispatcher = new SemanticModelEventDispatcher(bus, delegate);
    IllegalStateException failure = new IllegalStateException("decode failed");
    when(delegate.listMetrics(NAMESPACE)).thenThrow(failure);
    when(delegate.listMetricInfos(NAMESPACE)).thenThrow(failure);
    try (MockedStatic<PrincipalUtils> principal = mockStatic(PrincipalUtils.class)) {
      principal.when(PrincipalUtils::getCurrentUserName).thenReturn("initiator");
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () -> {
                if (details) dispatcher.listMetricInfos(NAMESPACE);
                else dispatcher.listMetrics(NAMESPACE);
              }));
    }
    List<BaseEvent> events = events(bus, details);
    assertEquals(
        details
            ? ListSemanticModelMetricInfosFailureEvent.class
            : ListSemanticModelMetricFailureEvent.class,
        events.get(1).getClass());
    assertSame(failure, ((FailureEvent) events.get(1)).exception());
    assertEquals(OperationStatus.FAILURE, ((Event) events.get(1)).operationStatus());
    assertEquals(
        NAMESPACE,
        details
            ? ((ListSemanticModelMetricInfosFailureEvent) events.get(1)).namespace()
            : ((ListSemanticModelMetricFailureEvent) events.get(1)).namespace());
  }

  private List<BaseEvent> events(EventBus bus, boolean details) {
    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(bus, times(2)).dispatchEvent(captor.capture());
    List<BaseEvent> events = captor.getAllValues();
    assertEquals(
        details
            ? ListSemanticModelMetricInfosPreEvent.class
            : ListSemanticModelMetricPreEvent.class,
        events.get(0).getClass());
    assertEquals(
        NAMESPACE,
        details
            ? ((ListSemanticModelMetricInfosPreEvent) events.get(0)).namespace()
            : ((ListSemanticModelMetricPreEvent) events.get(0)).namespace());
    for (BaseEvent event : events) {
      assertEquals("initiator", event.user());
      assertEquals(NameIdentifier.of(NAMESPACE.levels()), event.identifier());
      assertEquals(
          details
              ? OperationType.LIST_SEMANTIC_MODEL_METRIC_INFOS
              : OperationType.LIST_SEMANTIC_MODEL_METRIC,
          event.operationType());
    }
    return events;
  }
}
