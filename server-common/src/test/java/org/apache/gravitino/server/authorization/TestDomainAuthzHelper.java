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
package org.apache.gravitino.server.authorization;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainMemberInfo;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.listener.DomainEventDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersPreEvent;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.server.web.HttpAuditFilter;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

class TestDomainAuthzHelper {

  private final GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
  private final Principal principal = () -> "steward";
  private final Set<String> owners = new HashSet<>();
  private final Set<String> allows = new HashSet<>();
  private final Set<String> denies = new HashSet<>();
  private MockedStatic<GravitinoEnv> environment;
  private GravitinoEnv env;
  private DomainEntity child;

  @BeforeEach
  void setUp() {
    env = mock(GravitinoEnv.class);
    Config config = mock(Config.class);
    when(config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
    when(config.get(Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE)).thenReturn(2);
    when(env.config()).thenReturn(config);
    when(env.gravitinoAuthorizer()).thenReturn(authorizer);
    environment = mockStatic(GravitinoEnv.class);
    environment.when(GravitinoEnv::getInstance).thenReturn(env);
    child = domain("Trading", "Finance", Domain.Status.PUBLISHED);
    when(authorizer.isOwner(any(), anyString(), any(), any()))
        .thenAnswer(c -> owners.contains(c.<MetadataObject>getArgument(2).fullName()));
    when(authorizer.authorize(any(), anyString(), any(), any(), any()))
        .thenAnswer(c -> allows.contains(key(c.getArgument(2), c.getArgument(3))));
    when(authorizer.deny(any(), anyString(), any(), any(), any()))
        .thenAnswer(c -> denies.contains(key(c.getArgument(2), c.getArgument(3))));
  }

  @AfterEach
  void tearDown() {
    environment.close();
    RequestContext.clear();
  }

  @ParameterizedTest
  @ValueSource(strings = {"lake", "Finance", "Trading"})
  void ownerAtAnyScopeOverridesEveryDeny(String owner) {
    owners.add(owner);
    for (MetadataObject scope : DomainAuthzHelper.scopes("lake", child)) {
      denies.add(key(scope, Privilege.Name.MANAGE_DOMAIN));
      denies.add(key(scope, Privilege.Name.USE_DOMAIN));
    }
    assertTrue(permitted(Privilege.Name.MANAGE_DOMAIN));
    assertTrue(permitted(Privilege.Name.USE_DOMAIN));
  }

  @Test
  void deniesOverrideAllowsAcrossEveryScopeInBothDirections() {
    List<MetadataObject> scopes = DomainAuthzHelper.scopes("lake", child);
    for (MetadataObject allowed : scopes) {
      for (MetadataObject denied : scopes) {
        allows.clear();
        denies.clear();
        allows.add(key(allowed, Privilege.Name.USE_DOMAIN));
        assertTrue(permitted(Privilege.Name.USE_DOMAIN));
        denies.add(key(denied, Privilege.Name.USE_DOMAIN));
        assertFalse(permitted(Privilege.Name.USE_DOMAIN));
      }
    }
  }

  @Test
  void managementImpliesUsageAndUsageDeniesStillWin() {
    allows.add("Finance:MANAGE_DOMAIN");
    assertTrue(permitted(Privilege.Name.MANAGE_DOMAIN));
    assertTrue(permitted(Privilege.Name.USE_DOMAIN));
    denies.add("Trading:USE_DOMAIN");
    assertFalse(permitted(Privilege.Name.USE_DOMAIN));
    assertTrue(permitted(Privilege.Name.MANAGE_DOMAIN));
    DomainAuthzHelper.requireManage("lake", child);
    denies.add("Trading:MANAGE_DOMAIN");
    assertFalse(permitted(Privilege.Name.MANAGE_DOMAIN));
  }

  @Test
  void draftVisibilityNeedsManagementAndUsesNotFound() {
    DomainEntity draft = child.toBuilder().withStatus(Domain.Status.DRAFT).build();
    allows.add("Finance:USE_DOMAIN");
    assertTrue(DomainAuthzHelper.visible("lake", child));
    assertFalse(DomainAuthzHelper.visible("lake", draft));
    assertThrows(
        NoSuchDomainException.class, () -> DomainAuthzHelper.requireVisible("lake", draft));
    assertThrows(NoSuchDomainException.class, () -> DomainAuthzHelper.requireManage("lake", draft));
    assertThrows(ForbiddenException.class, () -> DomainAuthzHelper.requireManage("lake", child));
    allows.add("Finance:MANAGE_DOMAIN");
    assertTrue(DomainAuthzHelper.visible("lake", draft));
    assertDoesNotThrow(() -> DomainAuthzHelper.requireVisible("lake", draft));
    allows.clear();
    assertThrows(ForbiddenException.class, () -> DomainAuthzHelper.requireVisible("lake", child));
  }

  @Test
  void moveAndDetachUseOnlyCurrentParent() {
    allows.add("Finance:MANAGE_DOMAIN");
    assertTrue(DomainAuthzHelper.visible("lake", child));
    DomainEntity moved = child.toBuilder().withParent("Markets").build();
    assertFalse(DomainAuthzHelper.visible("lake", moved));
    allows.add("Markets:MANAGE_DOMAIN");
    assertTrue(DomainAuthzHelper.visible("lake", moved));
    assertFalse(DomainAuthzHelper.visible("lake", moved.toBuilder().withParent(null).build()));
    DomainDispatcher dispatcher = mock(DomainDispatcher.class);
    when(env.internalDomainDispatcher()).thenReturn(dispatcher);
    when(dispatcher.getDomain("lake", "Trading"))
        .thenReturn(child, moved, moved.toBuilder().withParent(null).build());
    MetadataObject object = MetadataObjects.of(null, "Trading", MetadataObject.Type.DOMAIN);
    assertEquals("Finance", DomainAuthzHelper.parent("lake", object).fullName());
    assertEquals("Markets", DomainAuthzHelper.parent("lake", object).fullName());
    assertNull(DomainAuthzHelper.parent("lake", object));
  }

  @Test
  void backingTagNeedsOwnershipOrCreateTagPrivilege() {
    allows.add("lake:MANAGE_DOMAIN");
    assertThrows(
        ForbiddenException.class, () -> DomainAuthzHelper.requireBackingTag("lake", "tag", false));
    assertThrows(
        ForbiddenException.class, () -> DomainAuthzHelper.requireBackingTag("lake", "tag", true));
    allows.add("lake:CREATE_TAG");
    assertDoesNotThrow(() -> DomainAuthzHelper.requireBackingTag("lake", "tag", true));
    assertThrows(
        ForbiddenException.class, () -> DomainAuthzHelper.requireBackingTag("lake", "tag", false));
    owners.add("tag");
    assertDoesNotThrow(() -> DomainAuthzHelper.requireBackingTag("lake", "tag", false));
    owners.clear();
    owners.add("lake");
    assertDoesNotThrow(() -> DomainAuthzHelper.requireBackingTag("lake", "another", false));
  }

  @Test
  void memberAccessUsesObjectPermissionWithoutTagPermission() {
    MetadataObject table = MetadataObjects.parse("cat.db.trades", MetadataObject.Type.TABLE);
    assertFalse(DomainAuthzHelper.canAccessObject("lake", table));
    allows.add("lake:USE_CATALOG");
    allows.add("lake:USE_SCHEMA");
    allows.add("lake:SELECT_TABLE");
    assertTrue(DomainAuthzHelper.canAccessObject("lake", table));
    verify(authorizer, never())
        .authorize(any(), anyString(), any(), eq(Privilege.Name.VIEW_TAG), any());
    verify(authorizer, never())
        .authorize(any(), anyString(), any(), eq(Privilege.Name.APPLY_TAG), any());
  }

  @Test
  void missingAuthorizerFailsClosedWhenEnabled() {
    when(env.gravitinoAuthorizer()).thenReturn(null);
    assertFalse(DomainAuthzHelper.visible("lake", child));
    assertThrows(
        ForbiddenException.class, () -> DomainAuthzHelper.requireBackingTag("lake", "tag", true));
  }

  @Test
  void readFilteringSharesContextButManagementChecksRemainIndependent() {
    Set<AuthorizationRequestContext> contexts = new HashSet<>();
    doAnswer(
            call -> {
              contexts.add(call.getArgument(3));
              return false;
            })
        .when(authorizer)
        .isOwner(any(), anyString(), any(), any());
    allows.add("lake:USE_DOMAIN");
    DomainEntity[] domains = {domain("Finance", null, Domain.Status.PUBLISHED), child};
    assertEquals(2, DomainAuthzHelper.filterDomains("lake", domains).length);
    assertEquals(1, contexts.size());
    contexts.clear();
    allows.add("lake:MANAGE_DOMAIN");
    DomainAuthzHelper.requireManage("lake", child);
    DomainAuthzHelper.requireManage("lake", child);
    assertEquals(2, contexts.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"unknown", "Secret"})
  void emptyAuthorizedMemberScopeStillEmitsQueryEvents(String scope) throws Exception {
    allows.add("Finance:USE_DOMAIN");
    DomainEntity finance = domain("Finance", null, Domain.Status.PUBLISHED);
    DomainDispatcher delegate = memberDispatcher(finance);
    DomainMemberFilter filter =
        DomainMemberFilter.builder()
            .withDomains(scope)
            .withTypes(MetadataObject.Type.TABLE)
            .withNameContains("trades")
            .build();
    when(delegate.listMembers(eq("lake"), eq(finance), any(DomainEntity[].class), eq(filter)))
        .thenReturn(new DomainMember[0]);
    List<BaseEvent> events = new ArrayList<>();
    EventListenerPlugin listener = recordingListener(events);
    EventBus bus = new EventBus(Collections.singletonList(listener));
    DomainEventDispatcher dispatcher = new DomainEventDispatcher(bus, delegate);
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn("/api/metalakes/lake/domains/Finance/objects");
    when(request.getDispatcherType()).thenReturn(DispatcherType.REQUEST);
    new HttpAuditFilter(bus, EventSource.GRAVITINO_SERVER)
        .doFilter(
            request,
            mock(HttpServletResponse.class),
            (req, response) ->
                assertEquals(
                    0,
                    DomainAuthzHelper.listMembers(dispatcher, "lake", "Finance", filter).length));
    assertEquals(2, events.size());
    assertSame(filter, ((ListDomainMembersPreEvent) events.get(0)).filter());
    ListDomainMembersEvent success = (ListDomainMembersEvent) events.get(1);
    assertSame(filter, success.filter());
    assertEquals(0, success.resultCount());
    verify(delegate)
        .listMembers(
            eq("lake"),
            eq(finance),
            argThat(snapshots -> snapshots.length == 1 && child.equals(snapshots[0])),
            eq(filter));
  }

  @Test
  void mixedMemberScopesKeepOnlyVisibleSnapshotsAndRelations() {
    allows.addAll(
        Arrays.asList(
            "Finance:USE_DOMAIN", "lake:USE_CATALOG", "lake:USE_SCHEMA", "lake:SELECT_TABLE"));
    DomainEntity finance = domain("Finance", null, Domain.Status.PUBLISHED);
    DomainDispatcher delegate = memberDispatcher(finance);
    MetadataObject table = MetadataObjects.parse("cat.db.trades", MetadataObject.Type.TABLE);
    DomainMember member =
        new DomainMemberInfo(
            table,
            new DomainRelation[] {
              DomainRelation.viaSubdomain("Trading"), DomainRelation.viaSubdomain("Secret")
            });
    DomainMemberFilter requested =
        DomainMemberFilter.builder()
            .withDomains("unknown", "Trading", "Secret")
            .withTypes(MetadataObject.Type.TABLE)
            .withNameContains("trades")
            .build();
    DomainMemberFilter effective =
        DomainMemberFilter.builder()
            .withDomains("Trading")
            .withTypes(MetadataObject.Type.TABLE)
            .withNameContains("trades")
            .build();
    when(delegate.listMembers(eq("lake"), eq(finance), any(DomainEntity[].class), eq(effective)))
        .thenReturn(new DomainMember[] {member});
    List<BaseEvent> events = new ArrayList<>();
    DomainDispatcher dispatcher =
        new DomainEventDispatcher(
            new EventBus(Collections.singletonList(recordingListener(events))), delegate);
    DomainMember[] result = DomainAuthzHelper.listMembers(dispatcher, "lake", "Finance", requested);
    assertEquals(1, result.length);
    assertEquals(table, result[0].object());
    assertEquals(1, result[0].relations().length);
    assertEquals(DomainRelation.viaSubdomain("Trading"), result[0].relations()[0]);
    assertEquals(effective, ((ListDomainMembersPreEvent) events.get(0)).filter());
    assertEquals(1, ((ListDomainMembersEvent) events.get(1)).resultCount());
    verify(delegate)
        .listMembers(
            eq("lake"),
            eq(finance),
            argThat(snapshots -> snapshots.length == 1 && child.equals(snapshots[0])),
            eq(effective));
  }

  @Test
  void preListenerCanRejectAnEmptyAuthorizedMemberScope() {
    allows.add("Finance:USE_DOMAIN");
    DomainEntity finance = domain("Finance", null, Domain.Status.PUBLISHED);
    DomainDispatcher delegate = memberDispatcher(finance);
    List<BaseEvent> events = new ArrayList<>();
    EventListenerPlugin listener = recordingListener(events);
    ForbiddenException rejected = new ForbiddenException("member query denied");
    doAnswer(
            call -> {
              BaseEvent event = call.getArgument(0);
              events.add(event);
              if (event instanceof ListDomainMembersPreEvent) {
                throw rejected;
              }
              return null;
            })
        .when(listener)
        .onPreEvent(any());
    DomainDispatcher dispatcher =
        new DomainEventDispatcher(new EventBus(Collections.singletonList(listener)), delegate);
    assertSame(
        rejected,
        assertThrows(
            ForbiddenException.class,
            () ->
                DomainAuthzHelper.listMembers(
                    dispatcher,
                    "lake",
                    "Finance",
                    DomainMemberFilter.builder().withDomains("unknown").build())));
    assertEquals(1, events.size());
    assertInstanceOf(ListDomainMembersPreEvent.class, events.get(0));
    verify(delegate, never())
        .listMembers(anyString(), any(DomainEntity.class), any(DomainEntity[].class), any());
  }

  private DomainDispatcher memberDispatcher(DomainEntity finance) {
    DomainDispatcher delegate = mock(DomainDispatcher.class);
    when(delegate.getDomain("lake", "Finance")).thenReturn(finance);
    when(delegate.listSubdomainInfos("lake", "Finance"))
        .thenReturn(new DomainEntity[] {child, domain("Secret", "Finance", Domain.Status.DRAFT)});
    when(env.internalDomainDispatcher()).thenReturn(delegate);
    return delegate;
  }

  private EventListenerPlugin recordingListener(List<BaseEvent> events) {
    EventListenerPlugin listener = mock(EventListenerPlugin.class);
    doAnswer(
            call -> {
              events.add(call.getArgument(0));
              return null;
            })
        .when(listener)
        .onPreEvent(any());
    doAnswer(
            call -> {
              events.add(call.getArgument(0));
              return null;
            })
        .when(listener)
        .onPostEvent(any());
    return listener;
  }

  private boolean permitted(Privilege.Name privilege) {
    return DomainAuthzHelper.allowed(
        authorizer,
        principal,
        "lake",
        DomainAuthzHelper.scopes("lake", child),
        privilege,
        new AuthorizationRequestContext());
  }

  private static String key(MetadataObject object, Privilege.Name privilege) {
    return object.fullName() + ":" + privilege.name();
  }

  private static DomainEntity domain(String name, String parent, Domain.Status status) {
    return DomainEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain("lake"))
        .withParent(parent)
        .withStatus(status)
        .withProperties(Collections.emptyMap())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
