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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Array;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.audit.JsonAuditFormatter;
import org.apache.gravitino.audit.SimpleFormatter;
import org.apache.gravitino.audit.v2.SimpleFormatterV2;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.listener.api.event.domain.AlterDomainEvent;
import org.apache.gravitino.listener.api.event.domain.AlterDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.AlterDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.AttachDomainEvent;
import org.apache.gravitino.listener.api.event.domain.AttachDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.AttachDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.CreateDomainEvent;
import org.apache.gravitino.listener.api.event.domain.CreateDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.CreateDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.DeleteDomainEvent;
import org.apache.gravitino.listener.api.event.domain.DeleteDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.DeleteDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.DetachDomainEvent;
import org.apache.gravitino.listener.api.event.domain.DetachDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.DetachDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.GetDomainEvent;
import org.apache.gravitino.listener.api.event.domain.GetDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.GetDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsInfoEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsInfoFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsInfoPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsInfoEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsInfoFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsInfoPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsPreEvent;
import org.apache.gravitino.listener.api.event.domain.PublishDomainEvent;
import org.apache.gravitino.listener.api.event.domain.PublishDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.PublishDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.UnpublishDomainEvent;
import org.apache.gravitino.listener.api.event.domain.UnpublishDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.UnpublishDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.UpdateDomainBackingTagEvent;
import org.apache.gravitino.listener.api.event.domain.UpdateDomainBackingTagFailureEvent;
import org.apache.gravitino.listener.api.event.domain.UpdateDomainBackingTagPreEvent;
import org.apache.gravitino.listener.api.info.DomainInfo;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestDomainEventDispatcher {
  private final String metalake = "lake/with spaces";
  private final String name = "Finance.US/a b";
  private final String parent = "Business.EU/parent";
  private final TagValue backingTag = TagValue.of("department", "finance");
  private final boolean createBackingTag = false;
  private final String comment = "comment";
  private final Map<String, String> properties = Collections.singletonMap("key", "value");
  private final DomainChange[] changes = new DomainChange[] {DomainChange.rename("renamed")};
  private final DomainMemberFilter filter =
      DomainMemberFilter.builder()
          .withDomains("Tax,EU", "Trading/US")
          .withTypes(MetadataObject.Type.TABLE)
          .withNameContains("a b&?")
          .build();
  private final MetadataObject object =
      MetadataObjects.parse("cat.schema.table", MetadataObject.Type.TABLE);
  private final DomainEntity domain =
      DomainEntity.builder()
          .withId(1L)
          .withName(name)
          .withNamespace(NamespaceUtil.ofDomain(metalake))
          .withProperties(properties)
          .withComment(comment)
          .withStatus(Domain.Status.PUBLISHED)
          .withParent(parent)
          .withBackingTagId(2L)
          .withBackingTagName(backingTag.name())
          .withBackingTagValue(backingTag.value().orElse(null))
          .withAuditInfo(
              AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
          .build();
  private final DomainEntity[] subdomains = new DomainEntity[0];
  private final List<BaseEvent> observed = new ArrayList<>();
  private EventListenerPlugin listener;
  private EventBus bus;

  @BeforeEach
  void setUp() {
    listener = mock(EventListenerPlugin.class);
    doAnswer(
            invocation -> {
              observed.add(invocation.getArgument(0));
              return null;
            })
        .when(listener)
        .onPreEvent(any());
    doAnswer(
            invocation -> {
              observed.add(invocation.getArgument(0));
              return null;
            })
        .when(listener)
        .onPostEvent(any());
    bus = new EventBus(Collections.singletonList(listener));
  }

  @AfterEach
  void clearContext() {
    RequestContext.clear();
  }

  @Test
  void testListDomains() {
    verifyOperation(
        d -> d.listDomains(metalake),
        new String[] {name},
        ListDomainsPreEvent.class,
        ListDomainsEvent.class,
        ListDomainsFailureEvent.class,
        OperationType.LIST_DOMAIN,
        NameIdentifier.of(metalake));
  }

  @Test
  void testListDomainInfos() {
    verifyOperation(
        d -> d.listDomainInfos(metalake),
        new DomainEntity[] {domain},
        ListDomainsInfoPreEvent.class,
        ListDomainsInfoEvent.class,
        ListDomainsInfoFailureEvent.class,
        OperationType.LIST_DOMAINS_INFO,
        NameIdentifier.of(metalake));
  }

  @Test
  void testGetDomain() {
    verifyOperation(
        d -> d.getDomain(metalake, name),
        domain,
        GetDomainPreEvent.class,
        GetDomainEvent.class,
        GetDomainFailureEvent.class,
        OperationType.GET_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testCreateDomain() {
    verifyOperation(
        d -> d.createDomain(metalake, name, comment, properties, backingTag, createBackingTag),
        domain,
        CreateDomainPreEvent.class,
        CreateDomainEvent.class,
        CreateDomainFailureEvent.class,
        OperationType.CREATE_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testCreateSubdomain() {
    verifyOperation(
        d ->
            d.createSubdomain(
                metalake, parent, name, comment, properties, backingTag, createBackingTag),
        domain,
        CreateDomainPreEvent.class,
        CreateDomainEvent.class,
        CreateDomainFailureEvent.class,
        OperationType.CREATE_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testAlterDomain() {
    verifyOperation(
        d -> d.alterDomain(metalake, name, changes),
        domain,
        AlterDomainPreEvent.class,
        AlterDomainEvent.class,
        AlterDomainFailureEvent.class,
        OperationType.ALTER_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testDeleteDomain() {
    verifyOperation(
        d -> d.deleteDomain(metalake, name),
        true,
        DeleteDomainPreEvent.class,
        DeleteDomainEvent.class,
        DeleteDomainFailureEvent.class,
        OperationType.DELETE_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testPublishDomain() {
    verifyOperation(
        d -> d.publishDomain(metalake, name),
        domain,
        PublishDomainPreEvent.class,
        PublishDomainEvent.class,
        PublishDomainFailureEvent.class,
        OperationType.PUBLISH_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testUnpublishDomain() {
    verifyOperation(
        d -> d.unpublishDomain(metalake, name),
        domain,
        UnpublishDomainPreEvent.class,
        UnpublishDomainEvent.class,
        UnpublishDomainFailureEvent.class,
        OperationType.UNPUBLISH_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testDetachDomain() {
    verifyOperation(
        d -> d.detachDomain(metalake, name),
        domain,
        DetachDomainPreEvent.class,
        DetachDomainEvent.class,
        DetachDomainFailureEvent.class,
        OperationType.DETACH_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testAttachDomain() {
    verifyOperation(
        d -> d.attachDomain(metalake, name, parent),
        domain,
        AttachDomainPreEvent.class,
        AttachDomainEvent.class,
        AttachDomainFailureEvent.class,
        OperationType.ATTACH_DOMAIN,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testUpdateBackingTag() {
    verifyOperation(
        d -> d.updateBackingTag(metalake, name, backingTag),
        domain,
        UpdateDomainBackingTagPreEvent.class,
        UpdateDomainBackingTagEvent.class,
        UpdateDomainBackingTagFailureEvent.class,
        OperationType.UPDATE_DOMAIN_BACKING_TAG,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testListSubdomains() {
    verifyOperation(
        d -> d.listSubdomains(metalake, name),
        new String[] {name},
        ListSubdomainsPreEvent.class,
        ListSubdomainsEvent.class,
        ListSubdomainsFailureEvent.class,
        OperationType.LIST_SUBDOMAINS,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testListSubdomainInfos() {
    verifyOperation(
        d -> d.listSubdomainInfos(metalake, name),
        new DomainEntity[] {domain},
        ListSubdomainsInfoPreEvent.class,
        ListSubdomainsInfoEvent.class,
        ListSubdomainsInfoFailureEvent.class,
        OperationType.LIST_SUBDOMAINS_INFO,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testListMembers() {
    verifyOperation(
        d -> d.listMembers(metalake, name, filter),
        new DomainMember[0],
        ListDomainMembersPreEvent.class,
        ListDomainMembersEvent.class,
        ListDomainMembersFailureEvent.class,
        OperationType.LIST_DOMAIN_MEMBERS,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testListMembersSnapshots() {
    verifyOperation(
        d -> d.listMembers(metalake, domain, subdomains, filter),
        new DomainMember[0],
        ListDomainMembersPreEvent.class,
        ListDomainMembersEvent.class,
        ListDomainMembersFailureEvent.class,
        OperationType.LIST_DOMAIN_MEMBERS,
        NameIdentifierUtil.ofDomain(metalake, name));
  }

  @Test
  void testListDomainsForMetadataObject() {
    verifyOperation(
        d -> d.listDomainsForMetadataObject(metalake, object),
        new DomainEntity[] {domain},
        ListDomainsForMetadataObjectPreEvent.class,
        ListDomainsForMetadataObjectEvent.class,
        ListDomainsForMetadataObjectFailureEvent.class,
        OperationType.LIST_DOMAINS_FOR_METADATA_OBJECT,
        MetadataObjectUtil.toEntityIdent(metalake, object));
  }

  @Test
  void testRequestAndResultFields() {
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    when(delegate.createSubdomain(metalake, parent, name, comment, properties, backingTag, false))
        .thenReturn(domain);
    DomainEventDispatcher dispatcher = new DomainEventDispatcher(bus, delegate);
    dispatcher.createSubdomain(metalake, parent, name, comment, properties, backingTag, false);
    CreateDomainPreEvent pre = (CreateDomainPreEvent) observed.get(0);
    assertInfo(pre.domainInfo(), Domain.Status.DRAFT, parent, null);
    assertFalse(pre.createBackingTag());
    CreateDomainEvent success = (CreateDomainEvent) observed.get(1);
    assertInfo(success.domainInfo(), Domain.Status.PUBLISHED, parent, domain.auditInfo());
    assertFalse(success.createBackingTag());
    observed.clear();
    DomainEntity unbound =
        domain.toBuilder()
            .withBackingTagId(null)
            .withBackingTagName(null)
            .withBackingTagValue(null)
            .build();
    when(delegate.updateBackingTag(metalake, name, null)).thenReturn(unbound);
    dispatcher.updateBackingTag(metalake, name, null);
    assertNull(((UpdateDomainBackingTagPreEvent) observed.get(0)).backingTag());
    assertNull(((UpdateDomainBackingTagEvent) observed.get(1)).backingTag());
    assertFalse(
        ((UpdateDomainBackingTagEvent) observed.get(1)).domainInfo().backingTag().isPresent());
    observed.clear();
    when(delegate.attachDomain(metalake, name, parent)).thenReturn(domain);
    dispatcher.attachDomain(metalake, name, parent);
    assertEquals(parent, ((AttachDomainPreEvent) observed.get(0)).parent());
    assertEquals(parent, ((AttachDomainEvent) observed.get(1)).parent());
    observed.clear();
    when(delegate.listMembers(metalake, name, filter)).thenReturn(new DomainMember[0]);
    dispatcher.listMembers(metalake, name, filter);
    assertSame(filter, ((ListDomainMembersPreEvent) observed.get(0)).filter());
    assertSame(filter, ((ListDomainMembersEvent) observed.get(1)).filter());
    observed.clear();
    when(delegate.listDomainsForMetadataObject(metalake, object))
        .thenReturn(new DomainEntity[] {domain});
    dispatcher.listDomainsForMetadataObject(metalake, object);
    assertSame(object, ((ListDomainsForMetadataObjectPreEvent) observed.get(0)).metadataObject());
    assertSame(object, ((ListDomainsForMetadataObjectEvent) observed.get(1)).metadataObject());
    observed.clear();
    when(delegate.alterDomain(metalake, name, changes)).thenReturn(domain);
    dispatcher.alterDomain(metalake, name, changes);
    DomainChange[] copy = ((AlterDomainPreEvent) observed.get(0)).changes();
    copy[0] = DomainChange.updateComment("mutated");
    assertEquals(changes[0], ((AlterDomainPreEvent) observed.get(0)).changes()[0]);
    assertArrayEquals(changes, ((AlterDomainEvent) observed.get(1)).changes());
  }

  @Test
  void testMutationChecksRemainInsideEvents() {
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    DomainDispatcher checked = mock(DomainDispatcher.class);
    Consumer<LockedDomainState> check =
        state -> {
          throw new ForbiddenException("denied");
        };
    when(delegate.withMutationCheck(check)).thenReturn(checked);
    ForbiddenException denied = new ForbiddenException("denied");
    when(checked.publishDomain(metalake, name)).thenThrow(denied);
    DomainDispatcher scoped = new DomainEventDispatcher(bus, delegate).withMutationCheck(check);
    assertTrue(observed.isEmpty());
    assertSame(
        denied, assertThrows(ForbiddenException.class, () -> scoped.publishDomain(metalake, name)));
    assertInstanceOf(PublishDomainPreEvent.class, observed.get(0));
    assertSame(denied, ((PublishDomainFailureEvent) observed.get(1)).exception());
    verify(delegate, never()).publishDomain(any(), any());
  }

  @Test
  void testPreListenerCanPreventMutation() {
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    ForbiddenException denied = new ForbiddenException("listener denied");
    doThrow(denied).when(listener).onPreEvent(any());
    assertSame(
        denied,
        assertThrows(
            ForbiddenException.class,
            () -> new DomainEventDispatcher(bus, delegate).deleteDomain(metalake, name)));
    verifyNoInteractions(delegate);
    verify(listener, never()).onPostEvent(any());
  }

  @Test
  void testFailureListenerCannotMaskOriginalError() {
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    NoSuchDomainException original = new NoSuchDomainException("hidden draft");
    when(delegate.getDomain(metalake, name)).thenThrow(original);
    doThrow(new RuntimeException("listener failed")).when(listener).onPostEvent(any());
    assertSame(
        original,
        assertThrows(
            NoSuchDomainException.class,
            () -> new DomainEventDispatcher(bus, delegate).getDomain(metalake, name)));
  }

  @Test
  void testSuccessListenerFailureFollowsExistingDispatcherContract() {
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    when(delegate.getDomain(metalake, name)).thenReturn(domain);
    RuntimeException failed = new RuntimeException("post listener failed");
    doAnswer(
            invocation -> {
              Event event = invocation.getArgument(0);
              observed.add(event);
              if (!(event instanceof FailureEvent)) {
                throw failed;
              }
              return null;
            })
        .when(listener)
        .onPostEvent(any());
    assertSame(
        failed,
        assertThrows(
            RuntimeException.class,
            () -> new DomainEventDispatcher(bus, delegate).getDomain(metalake, name)));
    assertEquals(3, observed.size());
    assertInstanceOf(GetDomainEvent.class, observed.get(1));
    assertSame(failed, ((GetDomainFailureEvent) observed.get(2)).exception());
  }

  @Test
  void testNullListAndAbsentDeleteResults() {
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    DomainEventDispatcher dispatcher = new DomainEventDispatcher(bus, delegate);
    assertNull(dispatcher.listDomains(metalake));
    assertEquals(-1, ((ListEvent) observed.get(1)).resultCount());
    observed.clear();
    assertFalse(dispatcher.deleteDomain(metalake, name));
    assertFalse(((DeleteDomainEvent) observed.get(1)).deleted());
  }

  @Test
  void testRequestContextIsCapturedForAsyncAudit() throws Exception {
    RequestContext.setRemoteAddress("192.0.2.10");
    RequestContext.setRequestQueryParams(Collections.singletonMap("nameContains", "a b&?"));
    ListDomainMembersEvent event = new ListDomainMembersEvent("user", metalake, name, filter, 0);
    RequestContext.clear();
    JsonNode json =
        JsonUtils.objectMapper().readTree(new JsonAuditFormatter().format(event).toString());
    assertEquals("192.0.2.10", json.get("remoteAddress").asText());
    assertEquals("a b&?", json.get("customInfo").get("nameContains").asText());
    assertEquals(0, json.get("resultCount").asInt());
    assertTrue(new SimpleFormatterV2().format(event).toString().contains("count=0"));
  }

  @Test
  void testDomainInfoIsIndependentOfMutableProperties() {
    Map<String, String> mutable = new HashMap<>(properties);
    DomainInfo info = new DomainInfo(name, null, mutable, Domain.Status.DRAFT, null, null, null);
    mutable.clear();
    assertEquals(properties, info.properties());
    assertThrows(UnsupportedOperationException.class, () -> info.properties().clear());
    assertNull(info.comment());
    assertFalse(info.parent().isPresent());
    assertFalse(info.backingTag().isPresent());
    assertNull(info.auditInfo());
    assertTrue(
        new DomainInfo(name, null, null, Domain.Status.DRAFT, null, null, null)
            .properties()
            .isEmpty());
  }

  private void verifyOperation(
      Function<DomainDispatcher, Object> invoke,
      Object expected,
      Class<? extends PreEvent> preType,
      Class<? extends Event> successType,
      Class<? extends FailureEvent> failureType,
      OperationType operation,
      NameIdentifier identifier) {
    DomainDispatcher delegate =
        mock(
            DomainDispatcher.class,
            invocation -> {
              assertEquals(1, observed.size(), "pre-event must precede the delegate");
              return expected;
            });
    Object result = invoke.apply(new DomainEventDispatcher(bus, delegate));
    assertEquals(expected, result);
    assertEquals(2, observed.size());
    assertEvent(observed.get(0), preType, operation, identifier, OperationStatus.UNPROCESSED);
    assertEvent(observed.get(1), successType, operation, identifier, OperationStatus.SUCCESS);
    if (observed.get(1) instanceof ListEvent) {
      assertEquals(
          expected == null ? -1 : Array.getLength(expected),
          ((ListEvent) observed.get(1)).resultCount());
    }
    assertEquals(1, mockingDetails(delegate).getInvocations().size());
    observed.clear();
    NoSuchDomainException failure = new NoSuchDomainException("operation failed");
    DomainDispatcher failing =
        mock(
            DomainDispatcher.class,
            invocation -> {
              assertEquals(1, observed.size());
              throw failure;
            });
    assertSame(
        failure,
        assertThrows(
            NoSuchDomainException.class,
            () -> invoke.apply(new DomainEventDispatcher(bus, failing))));
    assertEquals(2, observed.size());
    assertEvent(observed.get(0), preType, operation, identifier, OperationStatus.UNPROCESSED);
    assertEvent(observed.get(1), failureType, operation, identifier, OperationStatus.FAILURE);
    assertSame(failure, ((FailureEvent) observed.get(1)).exception());
  }

  private void assertEvent(
      BaseEvent event,
      Class<? extends BaseEvent> type,
      OperationType operation,
      NameIdentifier identifier,
      OperationStatus status) {
    assertEquals(type, event.getClass());
    assertEquals(operation, event.operationType());
    assertEquals(status, event.operationStatus());
    assertEquals(identifier, event.identifier());
    assertEquals(PrincipalUtils.getCurrentUserName(), event.user());
    assertEquals(EventSource.GRAVITINO_SERVER, event.eventSource());
    if (event instanceof Event) {
      assertAudit((Event) event);
    }
  }

  @SuppressWarnings("deprecation")
  private void assertAudit(Event event) {
    assertEquals(
        event.operationType().name(), new SimpleFormatter().format(event).operation().name());
    assertEquals(
        event.operationType().name(), new SimpleFormatterV2().format(event).operation().name());
    assertEquals(event.operationStatus(), new SimpleFormatterV2().format(event).operationStatus());
    try {
      JsonNode json =
          JsonUtils.objectMapper().readTree(new JsonAuditFormatter().format(event).toString());
      assertEquals(event.operationType().name(), json.get("operationType").asText());
      assertEquals(event.operationType().name(), json.get("operation").asText());
      assertEquals(event.operationStatus().name(), json.get("operationStatus").asText());
      assertEquals(event.identifier().toString(), json.get("identifier").asText());
      if (event instanceof ListEvent && ((ListEvent) event).resultCount() >= 0) {
        assertEquals(((ListEvent) event).resultCount(), json.get("resultCount").asInt());
      }
    } catch (Exception e) {
      throw new AssertionError("Domain audit JSON must be readable", e);
    }
  }

  private void assertInfo(
      DomainInfo info, Domain.Status status, String expectedParent, Audit audit) {
    assertEquals(name, info.name());
    assertEquals(comment, info.comment());
    assertEquals(properties, info.properties());
    assertEquals(status, info.status());
    assertEquals(expectedParent, info.parent().orElse(null));
    assertEquals(backingTag, info.backingTag().orElse(null));
    assertEquals(audit, info.auditInfo());
  }
}
