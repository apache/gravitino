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
package org.apache.gravitino.domain;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

/**
 * Tests {@link DomainManager} against a mocked entity store: how store exceptions map to domain
 * exceptions, and which check each store operation gets.
 */
public class TestDomainManagerWithMockStore {

  private static final String METALAKE = "metalake";
  private static final NameIdentifier FINANCE = NameIdentifierUtil.ofDomain(METALAKE, "Finance");
  private static final NameIdentifier TRADING = NameIdentifierUtil.ofDomain(METALAKE, "Trading");

  private EntityStore entityStore;
  private SupportsDomainOperations domainOperations;
  private DomainManager domainManager;

  @BeforeEach
  public void setUp() throws IOException {
    entityStore = Mockito.mock(EntityStore.class);
    domainOperations = Mockito.mock(SupportsDomainOperations.class);
    Mockito.when(entityStore.domainOperations()).thenReturn(domainOperations);
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(1L)
            .withName(METALAKE)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(auditInfo())
            .build();
    Mockito.when(
            entityStore.get(
                NameIdentifierUtil.ofMetalake(METALAKE),
                Entity.EntityType.METALAKE,
                BaseMetalake.class))
        .thenReturn(metalake);

    IdGenerator idGenerator = Mockito.mock(IdGenerator.class);
    Mockito.when(idGenerator.nextId()).thenReturn(100L);
    domainManager = new DomainManager(idGenerator, entityStore);
  }

  @Test
  public void testMissingMetalake() throws IOException {
    Mockito.when(
            entityStore.get(
                NameIdentifierUtil.ofMetalake("missing"),
                Entity.EntityType.METALAKE,
                BaseMetalake.class))
        .thenThrow(new NoSuchEntityException("missing"));

    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> domainManager.listDomainInfos("missing"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> domainManager.getDomain("missing", "Finance"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.createDomain("missing", "Finance", null, null));
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.createSubdomain("missing", "Finance", "Trading", null, null));
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.alterDomain("missing", "Finance", DomainChange.updateComment("c")));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> domainManager.deleteDomain("missing", "Finance"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> domainManager.publishDomain("missing", "Finance"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> domainManager.unpublishDomain("missing", "Finance"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.listSubdomainInfos("missing", "Finance"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.attachDomain("missing", "Trading", "Finance"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> domainManager.detachDomain("missing", "Trading"));
    Mockito.verifyNoInteractions(domainOperations);
    Mockito.verify(entityStore, Mockito.never()).update(any(), any(), any(), any());
    Mockito.verify(entityStore, Mockito.never()).delete(any(), any());
  }

  @Test
  public void testListAndGetDomain() throws IOException {
    Mockito.when(
            entityStore.list(
                NamespaceUtil.ofDomain(METALAKE), DomainEntity.class, Entity.EntityType.DOMAIN))
        .thenThrow(new IOException("list failed"));
    assertIOExceptionWrapped(() -> domainManager.listDomainInfos(METALAKE));

    Mockito.when(entityStore.get(FINANCE, Entity.EntityType.DOMAIN, DomainEntity.class))
        .thenThrow(new NoSuchEntityException("missing"));
    Mockito.when(entityStore.get(TRADING, Entity.EntityType.DOMAIN, DomainEntity.class))
        .thenThrow(new IOException("get failed"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.getDomain(METALAKE, "Finance"));
    assertIOExceptionWrapped(() -> domainManager.getDomain(METALAKE, "Trading"));
  }

  @Test
  public void testCreateDomain() throws IOException {
    ArgumentCaptor<DomainEntity> domainCaptor = ArgumentCaptor.forClass(DomainEntity.class);
    ArgumentCaptor<Consumer<LockedDomainState>> checkCaptor = checkCaptor();
    DomainEntity created = domainManager.createDomain(METALAKE, "Finance", "comment", null);
    Mockito.verify(domainOperations).insertDomain(domainCaptor.capture(), checkCaptor.capture());

    DomainEntity inserted = domainCaptor.getValue();
    Assertions.assertSame(inserted, created);
    Assertions.assertEquals(100L, inserted.id());
    Assertions.assertEquals(FINANCE, inserted.nameIdentifier());
    Assertions.assertEquals("comment", inserted.comment());
    Assertions.assertTrue(inserted.properties().isEmpty());
    Assertions.assertEquals(Domain.Status.DRAFT, inserted.status());
    Assertions.assertFalse(inserted.parent().isPresent());
    assertIsNewDomainCheck(checkCaptor.getValue());

    Mockito.doThrow(new EntityAlreadyExistsException("exists"))
        .when(domainOperations)
        .insertDomain(any(), any());
    Assertions.assertThrows(
        DomainAlreadyExistsException.class,
        () -> domainManager.createDomain(METALAKE, "Finance", null, null));

    // The metalake was dropped after it was checked.
    Mockito.doThrow(new NoSuchEntityException("missing"))
        .when(domainOperations)
        .insertDomain(any(), any());
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.createDomain(METALAKE, "Finance", null, null));

    Mockito.doThrow(new IOException("insert failed"))
        .when(domainOperations)
        .insertDomain(any(), any());
    assertIOExceptionWrapped(() -> domainManager.createDomain(METALAKE, "Finance", null, null));
  }

  @Test
  public void testCreateSubdomain() throws IOException {
    ArgumentCaptor<DomainEntity> domainCaptor = ArgumentCaptor.forClass(DomainEntity.class);
    ArgumentCaptor<Consumer<LockedDomainState>> checkCaptor = checkCaptor();
    domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null);
    Mockito.verify(domainOperations).insertDomain(domainCaptor.capture(), checkCaptor.capture());
    Assertions.assertEquals(TRADING, domainCaptor.getValue().nameIdentifier());
    Assertions.assertEquals("Finance", domainCaptor.getValue().parent().get());
    assertIsNewDomainCheck(checkCaptor.getValue());

    // The sub-domain is returned with the parent's stored name, which the check reads under the
    // parent's lock, even when the request names it differently.
    Mockito.doAnswer(
            invocation -> {
              DomainEntity newDomain = invocation.getArgument(0);
              Consumer<LockedDomainState> check = invocation.getArgument(1);
              DomainEntity storedParent = domain("Finance", null, Domain.Status.DRAFT);
              check.accept(state(newDomain, storedParent));
              return null;
            })
        .when(domainOperations)
        .insertDomain(any(), any());
    DomainEntity desk = domainManager.createSubdomain(METALAKE, "Finance ", "Desk", null, null);
    Assertions.assertEquals("Finance", desk.parent().get());
    Assertions.assertEquals(NameIdentifierUtil.ofDomain(METALAKE, "Desk"), desk.nameIdentifier());

    // A check that throws reaches the caller unchanged.
    Mockito.doAnswer(TestDomainManagerWithMockStore::runInsertCheck)
        .when(domainOperations)
        .insertDomain(any(), any());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.createSubdomain(METALAKE, "Trading", "Desk", null, null));

    Mockito.doThrow(new NoSuchEntityException("missing"))
        .when(domainOperations)
        .insertDomain(any(), any());
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.createSubdomain(METALAKE, "missing", "Trading", null, null));

    Mockito.doThrow(new EntityAlreadyExistsException("exists"))
        .when(domainOperations)
        .insertDomain(any(), any());
    Assertions.assertThrows(
        DomainAlreadyExistsException.class,
        () -> domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null));

    Mockito.doThrow(new IOException("insert failed"))
        .when(domainOperations)
        .insertDomain(any(), any());
    assertIOExceptionWrapped(
        () -> domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null));

    // A domain as its own parent, and a null parent, are refused before the store is called.
    Mockito.clearInvocations(domainOperations);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.createSubdomain(METALAKE, "Finance", "Finance", null, null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.createSubdomain(METALAKE, null, "Trading", null, null));
    Mockito.verifyNoInteractions(domainOperations);
  }

  @Test
  public void testAlterDomain() throws IOException {
    Mockito.when(entityStore.update(eq(FINANCE), eq(DomainEntity.class), any(), any()))
        .thenThrow(new NoSuchEntityException("missing"))
        .thenThrow(new EntityAlreadyExistsException("exists"))
        .thenThrow(new IOException("update failed"));

    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.alterDomain(METALAKE, "Finance", DomainChange.updateComment("c")));
    DomainAlreadyExistsException e =
        Assertions.assertThrows(
            DomainAlreadyExistsException.class,
            () ->
                domainManager.alterDomain(
                    METALAKE,
                    "Finance",
                    DomainChange.rename("Markets"),
                    DomainChange.rename("Trading")));
    // The message names the last new name.
    Assertions.assertTrue(e.getMessage().contains("Domain Trading"), e.getMessage());
    assertIOExceptionWrapped(
        () -> domainManager.alterDomain(METALAKE, "Finance", DomainChange.updateComment("c")));
  }

  @Test
  public void testDeleteDomain() throws IOException {
    Mockito.when(entityStore.delete(FINANCE, Entity.EntityType.DOMAIN))
        .thenReturn(true)
        .thenReturn(false)
        .thenThrow(new NonEmptyEntityException("has sub-domains"))
        .thenThrow(new IOException("delete failed"));

    Assertions.assertTrue(domainManager.deleteDomain(METALAKE, "Finance"));
    Assertions.assertFalse(domainManager.deleteDomain(METALAKE, "Finance"));
    Assertions.assertThrows(
        NonEmptyDomainException.class, () -> domainManager.deleteDomain(METALAKE, "Finance"));
    assertIOExceptionWrapped(() -> domainManager.deleteDomain(METALAKE, "Finance"));
  }

  @Test
  public void testPublishAndUnpublish() throws IOException {
    ArgumentCaptor<Consumer<LockedDomainState>> publishCheck = checkCaptor();
    domainManager.publishDomain(METALAKE, "Trading");
    Mockito.verify(domainOperations)
        .updateDomainStatusAndGet(
            eq(TRADING), eq(Domain.Status.PUBLISHED), any(), publishCheck.capture());
    assertIsPublishCheck(publishCheck.getValue());

    ArgumentCaptor<Consumer<LockedDomainState>> unpublishCheck = checkCaptor();
    ArgumentCaptor<AuditInfo> auditCaptor = ArgumentCaptor.forClass(AuditInfo.class);
    domainManager.unpublishDomain(METALAKE, "Finance");
    Mockito.verify(domainOperations)
        .updateDomainStatusAndGet(
            eq(FINANCE), eq(Domain.Status.DRAFT), auditCaptor.capture(), unpublishCheck.capture());
    assertIsUnpublishCheck(unpublishCheck.getValue());
    Assertions.assertNotNull(auditCaptor.getValue().lastModifier());
    Assertions.assertNotNull(auditCaptor.getValue().lastModifiedTime());

    Mockito.when(domainOperations.updateDomainStatusAndGet(any(), any(), any(), any()))
        .thenThrow(new NoSuchEntityException("missing"))
        .thenThrow(new NoSuchEntityException("missing"))
        .thenThrow(new IOException("update failed"))
        .thenThrow(new IOException("update failed"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.publishDomain(METALAKE, "Finance"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.unpublishDomain(METALAKE, "Finance"));
    assertIOExceptionWrapped(() -> domainManager.publishDomain(METALAKE, "Finance"));
    assertIOExceptionWrapped(() -> domainManager.unpublishDomain(METALAKE, "Finance"));
  }

  @Test
  public void testListSubdomains() throws IOException {
    DomainEntity trading = domain("Trading", "Finance", Domain.Status.DRAFT);
    Mockito.when(domainOperations.listSubDomains(FINANCE))
        .thenReturn(Collections.singletonList(trading))
        .thenThrow(new NoSuchEntityException("missing"))
        .thenThrow(new IOException("list failed"));

    Assertions.assertArrayEquals(
        new DomainEntity[] {trading}, domainManager.listSubdomainInfos(METALAKE, "Finance"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.listSubdomainInfos(METALAKE, "Finance"));
    assertIOExceptionWrapped(() -> domainManager.listSubdomainInfos(METALAKE, "Finance"));
  }

  @Test
  public void testAttachDomain() throws IOException {
    ArgumentCaptor<Consumer<LockedDomainState>> checkCaptor = checkCaptor();
    ArgumentCaptor<AuditInfo> auditCaptor = ArgumentCaptor.forClass(AuditInfo.class);
    domainManager.attachDomain(METALAKE, "Trading", "Finance");
    Mockito.verify(domainOperations)
        .setDomainParentAndGet(
            eq(TRADING), eq(FINANCE), auditCaptor.capture(), checkCaptor.capture());
    assertIsAttachCheck(checkCaptor.getValue());
    // A new edge takes the creator, a moved one the modifier, so both are set.
    Assertions.assertNotNull(auditCaptor.getValue().creator());
    Assertions.assertNotNull(auditCaptor.getValue().lastModifier());

    Mockito.when(domainOperations.setDomainParentAndGet(any(), any(), any(), any()))
        .thenThrow(new NoSuchEntityException("missing"))
        .thenThrow(new IOException("update failed"));
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.attachDomain(METALAKE, "Trading", "Finance"));
    assertIOExceptionWrapped(() -> domainManager.attachDomain(METALAKE, "Trading", "Finance"));

    // A domain as its own parent, and a null parent, are refused before the store is called.
    Mockito.clearInvocations(domainOperations);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.attachDomain(METALAKE, "Finance", "Finance"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.attachDomain(METALAKE, "Trading", null));
    Mockito.verifyNoInteractions(domainOperations);
  }

  @Test
  public void testDetachDomain() throws IOException {
    DomainEntity trading = domain("Trading", null, Domain.Status.DRAFT);
    Mockito.when(domainOperations.removeDomainParentAndGet(eq(TRADING), any()))
        .thenReturn(trading)
        .thenThrow(new NoSuchEntityException("missing"))
        .thenThrow(new IOException("update failed"));

    Assertions.assertSame(trading, domainManager.detachDomain(METALAKE, "Trading"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.detachDomain(METALAKE, "Trading"));
    assertIOExceptionWrapped(() -> domainManager.detachDomain(METALAKE, "Trading"));
  }

  // The insert check refuses a parent that is itself a sub-domain.
  private static void assertIsNewDomainCheck(Consumer<LockedDomainState> check) {
    DomainEntity desk = domain("Desk", "Trading", Domain.Status.DRAFT);
    check.accept(state(desk, domain("Trading", null, Domain.Status.DRAFT)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> check.accept(state(desk, domain("Trading", "Finance", Domain.Status.DRAFT))));
  }

  // The publish check refuses a draft parent, which the unpublish check would accept.
  private static void assertIsPublishCheck(Consumer<LockedDomainState> check) {
    DomainEntity trading = domain("Trading", "Finance", Domain.Status.DRAFT);
    check.accept(state(trading, domain("Finance", null, Domain.Status.PUBLISHED)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> check.accept(state(trading, domain("Finance", null, Domain.Status.DRAFT))));
  }

  // The unpublish check refuses published sub-domains, which the publish check would accept.
  private static void assertIsUnpublishCheck(Consumer<LockedDomainState> check) {
    DomainEntity finance = domain("Finance", null, Domain.Status.PUBLISHED);
    check.accept(
        new LockedDomainState(
            finance, null, List.of(domain("Trading", "Finance", Domain.Status.DRAFT))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            check.accept(
                new LockedDomainState(
                    finance,
                    null,
                    List.of(domain("Trading", "Finance", Domain.Status.PUBLISHED)))));
  }

  // The attach check refuses a domain with sub-domains, which the insert check would accept.
  private static void assertIsAttachCheck(Consumer<LockedDomainState> check) {
    DomainEntity trading = domain("Trading", null, Domain.Status.DRAFT);
    DomainEntity finance = domain("Finance", null, Domain.Status.DRAFT);
    check.accept(state(trading, finance));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            check.accept(
                new LockedDomainState(
                    trading, finance, List.of(domain("Desk", "Trading", Domain.Status.DRAFT)))));
  }

  private static Object runInsertCheck(InvocationOnMock invocation) {
    DomainEntity domain = invocation.getArgument(0);
    Consumer<LockedDomainState> check = invocation.getArgument(1);
    // The parent of the new domain is itself a sub-domain.
    check.accept(state(domain, domain(domain.parent().get(), "Finance", Domain.Status.PUBLISHED)));
    return null;
  }

  @SuppressWarnings("unchecked")
  private static ArgumentCaptor<Consumer<LockedDomainState>> checkCaptor() {
    return ArgumentCaptor.forClass(Consumer.class);
  }

  private static void assertIOExceptionWrapped(Runnable call) {
    RuntimeException e = Assertions.assertThrows(RuntimeException.class, call::run);
    Assertions.assertInstanceOf(IOException.class, e.getCause());
  }

  private static LockedDomainState state(DomainEntity domain, DomainEntity parent) {
    return new LockedDomainState(domain, parent, Collections.emptyList());
  }

  private static DomainEntity domain(String name, String parent, Domain.Status status) {
    return DomainEntity.builder()
        .withId((long) name.hashCode())
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain(METALAKE))
        .withProperties(Collections.emptyMap())
        .withStatus(status)
        .withParent(parent)
        .withAuditInfo(auditInfo())
        .build();
  }

  private static AuditInfo auditInfo() {
    return AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  }
}
