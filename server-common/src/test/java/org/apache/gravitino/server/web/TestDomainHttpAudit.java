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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.DomainEventDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.domain.GetDomainEvent;
import org.apache.gravitino.listener.api.event.domain.PublishDomainFailureEvent;
import org.apache.gravitino.listener.api.event.server.HttpRequestFailureEvent;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestDomainHttpAudit {
  @AfterEach
  void clearContext() {
    RequestContext.clear();
  }

  @Test
  void testDomainReadDoesNotSuppressLaterHttpVisibilityDenial() throws Exception {
    List<Event> events = new ArrayList<>();
    EventBus bus = eventBus(events);
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    DomainEntity draft =
        DomainEntity.builder()
            .withId(1L)
            .withName("draft")
            .withNamespace(NamespaceUtil.ofDomain("lake"))
            .withProperties(Collections.emptyMap())
            .withStatus(Domain.Status.DRAFT)
            .withAuditInfo(AuditInfo.builder().withCreator("owner").build())
            .build();
    when(delegate.getDomain("lake", "draft")).thenReturn(draft);
    DomainEventDispatcher dispatcher = new DomainEventDispatcher(bus, delegate);
    HttpAuditFilter filter = new HttpAuditFilter(bus, EventSource.GRAVITINO_SERVER);
    filter.doFilter(
        request(),
        mock(HttpServletResponse.class),
        (request, response) -> {
          dispatcher.getDomain("lake", "draft");
          // REST visibility checks run after loading the snapshot. The final 404 must still be
          // audited.
          ((HttpServletResponse) response).setStatus(404);
        });
    assertEquals(2, events.size());
    assertInstanceOf(GetDomainEvent.class, events.get(0));
    assertInstanceOf(HttpRequestFailureEvent.class, events.get(1));
  }

  @Test
  void testDomainMutationFailureSuppressesDuplicateHttpFailure() throws Exception {
    List<Event> events = new ArrayList<>();
    EventBus bus = eventBus(events);
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    ForbiddenException denied = new ForbiddenException("manage denied");
    when(delegate.publishDomain("lake", "draft")).thenThrow(denied);
    DomainEventDispatcher dispatcher = new DomainEventDispatcher(bus, delegate);
    HttpAuditFilter filter = new HttpAuditFilter(bus, EventSource.GRAVITINO_SERVER);
    filter.doFilter(
        request(),
        mock(HttpServletResponse.class),
        (request, response) -> {
          try {
            dispatcher.publishDomain("lake", "draft");
          } catch (ForbiddenException e) {
            ((HttpServletResponse) response).setStatus(403);
          }
        });
    assertEquals(1, events.size());
    assertSame(denied, ((PublishDomainFailureEvent) events.get(0)).exception());
  }

  private EventBus eventBus(List<Event> events) {
    EventListenerPlugin listener = mock(EventListenerPlugin.class);
    doAnswer(
            invocation -> {
              events.add(invocation.getArgument(0));
              return null;
            })
        .when(listener)
        .onPostEvent(any());
    return new EventBus(Collections.singletonList(listener));
  }

  private HttpServletRequest request() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn("/api/metalakes/lake/domains/draft");
    when(request.getRemoteAddr()).thenReturn("192.0.2.10");
    when(request.getDispatcherType()).thenReturn(DispatcherType.REQUEST);
    return request;
  }
}
