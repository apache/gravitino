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
package org.apache.gravitino.server.web.rest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainManager;
import org.apache.gravitino.domain.DomainNormalizeDispatcher;
import org.apache.gravitino.dto.requests.DomainParentSetRequest;
import org.apache.gravitino.dto.responses.DomainResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.hook.DomainHookDispatcher;
import org.apache.gravitino.listener.DomainEventDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.server.HttpRequestFailureEvent;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.HttpAuditFilter;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.filter.GravitinoInterceptionService;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.RequestContext;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** REST permission tests against the real domain manager with mocked storage transactions. */
public class TestDomainAuthorizationOperations extends BaseOperationsTest {

  private static final String BASE = "/metalakes/lake/domains";
  private final Set<String> allows = ConcurrentHashMap.newKeySet();
  private final Set<String> denies = ConcurrentHashMap.newKeySet();
  private final Set<String> owners = ConcurrentHashMap.newKeySet();
  private final AtomicReference<DomainEntity> current = new AtomicReference<>();
  private final EntityStore store = mock(EntityStore.class);
  private final SupportsDomainOperations transactions = mock(SupportsDomainOperations.class);
  private final GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
  private final DomainManager manager = new DomainManager(mock(IdGenerator.class), store);
  private final DomainDispatcher publicDispatcher =
      new DomainNormalizeDispatcher(new DomainHookDispatcher(manager));
  private DomainDispatcher savedInternalDomainDispatcher;
  private Config savedConfig;
  private GravitinoAuthorizer savedAuthorizer;

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(new ObjectMapperProvider());
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ResourceConfig(
            DomainOperations.class,
            DomainMembershipOperations.class,
            MetadataObjectDomainOperations.class)
        .register(new ObjectMapperProvider())
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(publicDispatcher).to(DomainDispatcher.class);
                bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
              }
            });
  }

  @BeforeEach
  void configureAuthorization() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    savedConfig = env.config();
    savedInternalDomainDispatcher = env.internalDomainDispatcher();
    FieldUtils.writeField(
        env, "internalDomainDispatcher", new DomainNormalizeDispatcher(manager), true);
    savedAuthorizer = env.gravitinoAuthorizer();
    Config config = mock(Config.class);
    when(config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
    when(config.get(Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE)).thenReturn(2);
    FieldUtils.writeField(env, "config", config, true);
    env.setGravitinoAuthorizer(authorizer);
    current.set(domain("Trading", "Finance", Domain.Status.DRAFT));
    when(store.domainOperations()).thenReturn(transactions);
    when(store.get(NameIdentifierUtil.ofMetalake("lake"), EntityType.METALAKE, BaseMetalake.class))
        .thenReturn(
            BaseMetalake.builder()
                .withId(1L)
                .withName("lake")
                .withVersion(SchemaVersion.V_0_1)
                .withAuditInfo(
                    AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
                .build());
    when(store.get(any(NameIdentifier.class), eq(EntityType.DOMAIN), eq(DomainEntity.class)))
        .thenAnswer(call -> current.get());
    for (String parent : new String[] {"Finance", "Markets"}) {
      doReturn(domain(parent, null, Domain.Status.PUBLISHED))
          .when(store)
          .get(
              eq(NameIdentifierUtil.ofDomain("lake", parent)),
              eq(EntityType.DOMAIN),
              eq(DomainEntity.class));
    }
    when(store.list(NamespaceUtil.ofDomain("lake"), DomainEntity.class, EntityType.DOMAIN))
        .thenAnswer(call -> Arrays.asList(current.get()));
    when(authorizer.isOwner(any(), anyString(), any(), any()))
        .thenAnswer(call -> owners.contains(call.<MetadataObject>getArgument(2).fullName()));
    when(authorizer.authorize(any(), anyString(), any(), any(), any()))
        .thenAnswer(
            call ->
                allows.contains(
                    call.<MetadataObject>getArgument(2).fullName() + ":" + call.getArgument(3)));
    when(authorizer.deny(any(), anyString(), any(), any(), any()))
        .thenAnswer(
            call ->
                denies.contains(
                    call.<MetadataObject>getArgument(2).fullName() + ":" + call.getArgument(3)));
    doAnswer(
            call -> {
              DomainEntity inserted = call.getArgument(0);
              TagEntity tag =
                  TagEntity.builder()
                      .withId(10L)
                      .withName("tag")
                      .withNamespace(NamespaceUtil.ofTag("lake"))
                      .withProperties(Collections.emptyMap())
                      .withAuditInfo(inserted.auditInfo())
                      .withAllowedValues(new String[0])
                      .build();
              DomainEntity bound =
                  inserted.toBuilder().withBackingTagId(10L).withBackingTagName("tag").build();
              DomainEntity parent =
                  inserted
                      .parent()
                      .map(name -> domain(name, null, Domain.Status.PUBLISHED))
                      .orElse(null);
              call.<Consumer<LockedDomainState>>getArgument(2)
                  .accept(
                      new LockedDomainState(
                          bound, parent, Collections.emptyList(), tag, Collections.emptyList()));
              return null;
            })
        .when(transactions)
        .insertDomainWithNewBackingTag(any(), any(), any());
    when(transactions.updateDomainStatusAndGet(any(), any(), any(), any()))
        .thenAnswer(
            call -> {
              call.<Consumer<LockedDomainState>>getArgument(3).accept(state());
              current.set(current.get().toBuilder().withStatus(call.getArgument(1)).build());
              return current.get();
            });
    when(transactions.removeDomainParentAndGet(any(), any()))
        .thenAnswer(
            call -> {
              call.<Consumer<LockedDomainState>>getArgument(1).accept(state());
              current.set(current.get().toBuilder().withParent(null).build());
              return current.get();
            });
    when(transactions.setDomainParentAndGet(any(), any(), any(), any()))
        .thenAnswer(
            call -> {
              DomainEntity parent =
                  domain(call.<NameIdentifier>getArgument(1).name(), null, Domain.Status.PUBLISHED);
              call.<Consumer<LockedDomainState>>getArgument(3)
                  .accept(new LockedDomainState(current.get(), parent, Collections.emptyList()));
              current.set(current.get().toBuilder().withParent(parent.name()).build());
              return current.get();
            });
    when(transactions.updateDomainBackingTagAndGet(any(), any(), any(), any()))
        .thenAnswer(
            call -> {
              call.<Consumer<LockedDomainState>>getArgument(3).accept(state());
              return current.get();
            });
    when(transactions.deleteDomain(any(), any()))
        .thenAnswer(
            call -> {
              call.<Consumer<DomainEntity>>getArgument(1).accept(current.get());
              return true;
            });
  }

  @AfterEach
  void restoreAuthorization() throws Exception {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", savedConfig, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "internalDomainDispatcher",
        savedInternalDomainDispatcher,
        true);
    GravitinoEnv.getInstance().setGravitinoAuthorizer(savedAuthorizer);
    RequestContext.resetOperationFailureFired();
    RequestContext.clear();
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "Trading"})
  void missingParentChangePreflightRetainsHttpAuditContext(String name) throws Exception {
    allows.add("lake:MANAGE_DOMAIN");
    doThrow(new NoSuchEntityException("Missing domain"))
        .when(store)
        .get(
            eq(NameIdentifierUtil.ofDomain("lake", "missing")),
            eq(EntityType.DOMAIN),
            eq(DomainEntity.class));
    List<Event> events = new ArrayList<>();
    EventListenerPlugin listener = mock(EventListenerPlugin.class);
    doAnswer(
            call -> {
              events.add(call.getArgument(0));
              return null;
            })
        .when(listener)
        .onPostEvent(any());
    EventBus bus = new EventBus(Collections.singletonList(listener));
    DomainOperations operations =
        new DomainOperations(new DomainEventDispatcher(bus, publicDispatcher));
    HttpServletRequest request = mock(HttpServletRequest.class);
    String uri = "/api" + BASE + "/" + name + "/parent";
    when(request.getMethod()).thenReturn("PUT");
    when(request.getRequestURI()).thenReturn(uri);
    when(request.getRemoteAddr()).thenReturn("192.0.2.10");
    when(request.getDispatcherType()).thenReturn(DispatcherType.REQUEST);
    FieldUtils.writeField(operations, "httpRequest", request, true);
    new HttpAuditFilter(bus, EventSource.GRAVITINO_SERVER)
        .doFilter(
            request,
            mock(HttpServletResponse.class),
            (req, response) -> {
              try (Response result =
                  operations.setParent("lake", name, new DomainParentSetRequest("missing"))) {
                assertEquals(404, result.getStatus());
                ((HttpServletResponse) response).setStatus(result.getStatus());
              }
            });
    assertEquals(1, events.size());
    HttpRequestFailureEvent failure =
        assertInstanceOf(HttpRequestFailureEvent.class, events.get(0));
    assertEquals("PUT", failure.httpMethod());
    assertEquals(uri, failure.requestUri());
    assertEquals(404, failure.statusCode());
  }

  @Test
  void missingMemberDomainRetainsHttpAuditContext() throws Exception {
    allows.add("lake:USE_DOMAIN");
    doThrow(new NoSuchEntityException("Missing domain"))
        .when(store)
        .get(
            eq(NameIdentifierUtil.ofDomain("lake", "missing")),
            eq(EntityType.DOMAIN),
            eq(DomainEntity.class));
    List<Event> events = new ArrayList<>();
    EventListenerPlugin listener = mock(EventListenerPlugin.class);
    doAnswer(
            call -> {
              events.add(call.getArgument(0));
              return null;
            })
        .when(listener)
        .onPostEvent(any());
    EventBus bus = new EventBus(Collections.singletonList(listener));
    DomainMembershipOperations operations =
        new DomainMembershipOperations(new DomainEventDispatcher(bus, publicDispatcher));
    HttpServletRequest request = mock(HttpServletRequest.class);
    String uri = "/api" + BASE + "/missing/objects";
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn(uri);
    when(request.getRemoteAddr()).thenReturn("192.0.2.10");
    when(request.getDispatcherType()).thenReturn(DispatcherType.REQUEST);
    FieldUtils.writeField(operations, "httpRequest", request, true);
    new HttpAuditFilter(bus, EventSource.GRAVITINO_SERVER)
        .doFilter(
            request,
            mock(HttpServletResponse.class),
            (req, response) -> {
              try (Response result =
                  operations.listMembers(
                      "lake", "missing", null, null, Collections.emptyList(), false)) {
                assertEquals(404, result.getStatus());
                ((HttpServletResponse) response).setStatus(result.getStatus());
              }
            });
    assertEquals(1, events.size());
    HttpRequestFailureEvent failure =
        assertInstanceOf(HttpRequestFailureEvent.class, events.get(0));
    assertEquals("GET", failure.httpMethod());
    assertEquals(uri, failure.requestUri());
    assertEquals(404, failure.statusCode());
  }

  @Test
  void draftIsNotFoundAndOmittedEvenWithUsage() {
    allows.add("Finance:USE_DOMAIN");
    expect("GET", BASE + "/Trading", null, 404);
    expect("GET", BASE + "/Trading/objects", null, 404);
    expect("GET", BASE + "/Trading/subdomains", null, 404);
    expect("PATCH", BASE + "/Trading", "{\"status\":\"published\"}", 404);
    try (Response response = target(BASE).request().get()) {
      assertEquals(200, response.getStatus());
      assertArrayEquals(new String[0], response.readEntity(NameListResponse.class).getNames());
    }
    allows.add("Finance:MANAGE_DOMAIN");
    try (Response response = target(BASE + "/Trading").request().get()) {
      assertEquals(200, response.getStatus());
      assertEquals(
          Domain.Status.DRAFT, response.readEntity(DomainResponse.class).getDomain().getStatus());
    }
  }

  @Test
  void publishedUsageDenyAndOwnerException() {
    current.set(current.get().toBuilder().withStatus(Domain.Status.PUBLISHED).build());
    expect("GET", BASE + "/Trading", null, 403);
    expect("DELETE", BASE + "/Trading", null, 403);
    allows.add("Finance:USE_DOMAIN");
    expect("GET", BASE + "/Trading", null, 200);
    expect("PATCH", BASE + "/Trading", "{\"status\":\"draft\"}", 403);
    denies.add("Trading:USE_DOMAIN");
    expect("GET", BASE + "/Trading", null, 403);
    owners.add("Finance");
    expect("GET", BASE + "/Trading", null, 200);
    expect("PATCH", BASE + "/Trading", "{\"status\":\"draft\"}", 200);
  }

  @Test
  void parentOnlyManagerAndOwnerCanDeleteChildBeforeDetach() {
    for (Domain.Status status : Domain.Status.values()) {
      current.set(domain("Trading", "Finance", status));
      allows.add("Finance:MANAGE_DOMAIN");
      expect("DELETE", BASE + "/Trading", null, 200);
      allows.clear();
      owners.add("Finance");
      expect("DELETE", BASE + "/Trading", null, 200);
      owners.clear();
    }
  }

  @Test
  void missingAndHiddenDraftDeletesReturnNotDropped() throws Exception {
    doReturn(false)
        .when(transactions)
        .deleteDomain(eq(NameIdentifierUtil.ofDomain("lake", "missing")), any());
    expectDrop("missing", false);
    expectDrop("Trading", false);
    allows.add("Finance:USE_DOMAIN");
    expectDrop("missing", false);
    expectDrop("Trading", false);
    allows.add("Finance:MANAGE_DOMAIN");
    expectDrop("Trading", true);
    expectDrop("missing", false);
    allows.clear();
    allows.add("lake:MANAGE_DOMAIN");
    expectDrop("missing", false);
    expectDrop("Trading", true);
  }

  @Test
  void inheritedManagerCanDetachThenLosesAccess() {
    allows.add("Finance:MANAGE_DOMAIN");
    expect("DELETE", BASE + "/Trading/parent", null, 200);
    expect("GET", BASE + "/Trading", null, 404);
    expectDrop("Trading", false);
  }

  @Test
  void moveRequiresNewParentAndChangesInheritedAccess() {
    allows.add("Finance:MANAGE_DOMAIN");
    expect("PUT", BASE + "/Trading/parent", "{\"parent\":\"Markets\"}", 403);
    owners.add("Markets");
    expect("PUT", BASE + "/Trading/parent", "{\"parent\":\"Markets\"}", 200);
    owners.clear();
    expect("GET", BASE + "/Trading", null, 404);
    allows.add("Markets:MANAGE_DOMAIN");
    expect("GET", BASE + "/Trading", null, 200);
  }

  @Test
  void backingTagRequiresTagOwnershipAndRemovalOnlyNeedsManagement() {
    allows.add("Finance:MANAGE_DOMAIN");
    expect("PUT", BASE + "/Trading/backingTag", "{\"backingTag\":{\"name\":\"tag\"}}", 403);
    expect("PUT", BASE + "/Trading/backingTag", "{\"backingTag\":null}", 200);
  }

  @Test
  void createNeedsMetalakeManagementCreateTagAndParentManagement() {
    String topLevel = "{\"name\":\"New\",\"backingTag\":{\"name\":\"tag\",\"create\":true}}";
    String subdomain =
        "{\"name\":\"New\",\"parent\":\"Finance\",\"backingTag\":{\"name\":\"tag\",\"create\":true}}";
    expect("POST", BASE, topLevel, 403);
    allows.add("lake:MANAGE_DOMAIN");
    expect("POST", BASE, topLevel, 403);
    allows.add("lake:CREATE_TAG");
    expect("POST", BASE, topLevel, 200);
    denies.add("Finance:MANAGE_DOMAIN");
    expect("POST", BASE, subdomain, 403);
    denies.clear();
    expect("POST", BASE, subdomain, 200);
  }

  @Test
  void reverseLookupRequiresObjectAccess() {
    expect("GET", "/metalakes/lake/objects/table/cat.db.trades/domains", null, 403);
  }

  @Test
  public void testRejectsInvalidDomainPath() {
    try (Response response =
        target(BASE + "/{domain}").resolveTemplate("domain", "Finance/US").request().get()) {
      assertEquals(400, response.getStatus());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"*", "SyStEm", "bad.name", "a/b", " "})
  void invalidNewNamesAreRejectedBeforeStorage(String name) throws Exception {
    allows.add("lake:MANAGE_DOMAIN");
    allows.add("lake:CREATE_TAG");
    expect("GET", BASE + "/SyStEm", null, 400);
    String backingTag = "\"backingTag\":{\"name\":\"tag\",\"create\":true}";
    expect("POST", BASE, String.format("{\"name\":\"%s\",%s}", name, backingTag), 400);
    expect(
        "POST",
        BASE,
        String.format("{\"name\":\"%s\",\"parent\":\"Finance\",%s}", name, backingTag),
        400);
    expect(
        "PUT",
        BASE + "/Trading",
        String.format("{\"updates\":[{\"@type\":\"rename\",\"newName\":\"%s\"}]}", name),
        400);
    verify(transactions, never()).insertDomainWithNewBackingTag(any(), any(), any());
    verify(store, never()).update(any(), eq(DomainEntity.class), eq(EntityType.DOMAIN), any());
    expect("GET", BASE, null, 200);
  }

  @Test
  void hiddenParentIsRejectedBeforeBackingTagConflict() throws Exception {
    doReturn(domain("Finance", null, Domain.Status.DRAFT))
        .when(store)
        .get(
            eq(NameIdentifierUtil.ofDomain("lake", "Finance")),
            eq(EntityType.DOMAIN),
            eq(DomainEntity.class));
    doThrow(new NoSuchEntityException("Missing parent"))
        .when(store)
        .get(
            eq(NameIdentifierUtil.ofDomain("lake", "missing")),
            eq(EntityType.DOMAIN),
            eq(DomainEntity.class));
    doThrow(new TagAlreadyExistsException("Tag already exists"))
        .when(transactions)
        .insertDomainWithNewBackingTag(any(), any(), any());
    allows.add("lake:MANAGE_DOMAIN");
    allows.add("lake:CREATE_TAG");
    denies.add("Finance:MANAGE_DOMAIN");
    for (String parent : new String[] {"Finance", "missing"}) {
      expect(
          "POST",
          BASE,
          "{\"name\":\"New\",\"parent\":\""
              + parent
              + "\",\"backingTag\":{\"name\":\"tag\",\"create\":true}}",
          404);
    }
    verify(transactions, never()).insertDomainWithNewBackingTag(any(), any(), any());
  }

  @Test
  void hiddenSourceIsRejectedBeforeParentOrTagResolution() throws Exception {
    doThrow(new NoSuchEntityException("Missing domain"))
        .when(store)
        .get(
            eq(NameIdentifierUtil.ofDomain("lake", "missing")),
            eq(EntityType.DOMAIN),
            eq(DomainEntity.class));
    doThrow(new NoSuchEntityException("Missing parent"))
        .when(transactions)
        .setDomainParentAndGet(any(), any(), any(), any());
    doThrow(new NoSuchTagException("Missing tag"))
        .when(transactions)
        .updateDomainBackingTagAndGet(any(), any(), any(), any());
    for (String name : new String[] {"Trading", "missing"}) {
      expect("PUT", BASE + "/" + name + "/parent", "{\"parent\":\"unknown\"}", 404);
      expect(
          "PUT", BASE + "/" + name + "/backingTag", "{\"backingTag\":{\"name\":\"unknown\"}}", 404);
    }
    verify(transactions, never()).setDomainParentAndGet(any(), any(), any(), any());
    verify(transactions, never()).updateDomainBackingTagAndGet(any(), any(), any(), any());
  }

  @Test
  void createChecksMetalakeManagementBeforeNameCollision() throws Exception {
    current.set(domain("Hidden", null, Domain.Status.DRAFT));
    doThrow(new EntityAlreadyExistsException("Name already exists"))
        .when(transactions)
        .insertDomainWithNewBackingTag(any(), any(), any());
    String request = "{\"name\":\"Hidden\",\"backingTag\":{\"name\":\"tag\",\"create\":true}}";

    expect("POST", BASE, request, 403);
    verify(transactions, never()).insertDomainWithNewBackingTag(any(), any(), any());

    allows.add("lake:MANAGE_DOMAIN");
    allows.add("lake:CREATE_TAG");
    denies.add("Hidden:MANAGE_DOMAIN");
    expect("GET", BASE + "/Hidden", null, 404);
    expect("POST", BASE, request, 409);
    verify(transactions).insertDomainWithNewBackingTag(any(), any(), any());
  }

  @Test
  void renameChecksSourceManagementBeforeNameCollision() throws Exception {
    current.set(domain("Trading", "Finance", Domain.Status.PUBLISHED));
    doReturn(domain("Hidden", null, Domain.Status.DRAFT))
        .when(store)
        .get(
            eq(NameIdentifierUtil.ofDomain("lake", "Hidden")),
            eq(EntityType.DOMAIN),
            eq(DomainEntity.class));
    AtomicInteger collisionChecks = new AtomicInteger();
    when(store.update(any(), eq(DomainEntity.class), eq(EntityType.DOMAIN), any()))
        .thenAnswer(
            call -> {
              DomainEntity updated =
                  call.<Function<DomainEntity, DomainEntity>>getArgument(3).apply(current.get());
              assertEquals("Hidden", updated.name());
              collisionChecks.incrementAndGet();
              throw new EntityAlreadyExistsException("Name already exists");
            });
    String request = "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"Hidden\"}]}";

    expect("PUT", BASE + "/Trading", request, 403);
    assertEquals(0, collisionChecks.get());

    allows.add("Trading:MANAGE_DOMAIN");
    expect("GET", BASE + "/Hidden", null, 404);
    expect("PUT", BASE + "/Trading", request, 409);
    assertEquals(1, collisionChecks.get());
    assertEquals("Trading", current.get().name());
  }

  @Test
  void deleteMembershipCheckRunsBeforeMissingAndDraftMasking() throws Throwable {
    doReturn(false)
        .when(transactions)
        .deleteDomain(eq(NameIdentifierUtil.ofDomain("lake", "missing")), any());
    for (String name : new String[] {"missing", "Trading"}) {
      MethodInvocation invocation = deleteInvocation(name);
      try (Response response = interceptDelete(invocation)) {
        assertEquals(403, response.getStatus());
      }
      verify(invocation, never()).proceed();
    }
    verify(transactions, never()).deleteDomain(any(), any());

    when(authorizer.isMetalakeUser(eq("lake"), any())).thenReturn(true);
    for (String name : new String[] {"missing", "Trading"}) {
      MethodInvocation invocation = deleteInvocation(name);
      try (Response response = interceptDelete(invocation)) {
        assertEquals(200, response.getStatus());
        assertEquals(false, ((DropResponse) response.getEntity()).dropped());
      }
      verify(invocation).proceed();
    }

    current.set(current.get().toBuilder().withStatus(Domain.Status.PUBLISHED).build());
    try (Response response = interceptDelete(deleteInvocation("Trading"))) {
      assertEquals(403, response.getStatus());
    }
  }

  private MethodInvocation deleteInvocation(String name) throws Throwable {
    DomainOperations operations = new DomainOperations(publicDispatcher);
    FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
    Method method = DomainOperations.class.getMethod("deleteDomain", String.class, String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[] {"lake", name});
    when(invocation.proceed()).thenAnswer(ignored -> operations.deleteDomain("lake", name));
    return invocation;
  }

  private Response interceptDelete(MethodInvocation invocation) throws Throwable {
    return (Response)
        new GravitinoInterceptionService()
            .getMethodInterceptors(invocation.getMethod())
            .get(0)
            .invoke(invocation);
  }

  private LockedDomainState state() {
    DomainEntity child = current.get();
    return new LockedDomainState(
        child,
        child.parent().map(name -> domain(name, null, Domain.Status.PUBLISHED)).orElse(null),
        Collections.emptyList());
  }

  private void expectDrop(String name, boolean dropped) {
    try (Response response = target(BASE + "/" + name).request().delete()) {
      assertEquals(200, response.getStatus());
      assertEquals(dropped, response.readEntity(DropResponse.class).dropped());
    }
  }

  private void expect(String method, String path, String body, int status) {
    try (Response response =
        target(path)
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .request()
            .method(
                method,
                body == null ? null : Entity.entity(body, MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(status, response.getStatus());
      if (status >= 400) {
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
        assertEquals(
            status == 400
                ? "IllegalArgumentException"
                : status == 404
                    ? "NoSuchDomainException"
                    : status == 409 ? "DomainAlreadyExistsException" : "ForbiddenException",
            response.readEntity(ErrorResponse.class).getType());
      }
    }
  }

  private static DomainEntity domain(String name, String parent, Domain.Status status) {
    return DomainEntity.builder()
        .withId(name.equals("Trading") ? 2L : 1L)
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
