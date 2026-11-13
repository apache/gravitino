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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.exceptions.BackingTagAlreadyBoundException;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
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
  private static final TagValue BACKING_TAG = TagValue.of("dept", "finance");

  private EntityStore entityStore;
  private SupportsDomainOperations domainOperations;
  private DomainDispatcher domainManager;

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

    // Like the store, an insert runs its check, which the manager returns the domain from.
    Mockito.doAnswer(TestDomainManagerWithMockStore::runTopLevelInsertCheck)
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    Mockito.doAnswer(TestDomainManagerWithMockStore::runTopLevelInsertCheck)
        .when(domainOperations)
        .insertDomainWithNewBackingTag(any(), any(), any());

    IdGenerator idGenerator = Mockito.mock(IdGenerator.class);
    Mockito.when(idGenerator.nextId()).thenReturn(100L);
    domainManager = new DomainNormalizeDispatcher(new DomainManager(idGenerator, entityStore));
  }

  /** Verifies scoped caller checks run on locked state without changing the original dispatcher. */
  @Test
  public void testScopedMutationCheckUsesCurrentState() throws IOException {
    DomainEntity current = domain("Trading", "Markets", Domain.Status.DRAFT);
    Mockito.when(domainOperations.removeDomainParentAndGet(eq(TRADING), any()))
        .thenAnswer(
            call -> {
              call.<Consumer<LockedDomainState>>getArgument(1)
                  .accept(new LockedDomainState(current, null, Collections.emptyList()));
              return current.toBuilder().withParent(null).build();
            });
    Consumer<LockedDomainState> check = Mockito.mock(Consumer.class);
    Consumer<LockedDomainState> additional = Mockito.mock(Consumer.class);
    domainManager
        .withMutationCheck(check)
        .withMutationCheck(additional)
        .detachDomain(METALAKE, "Trading");
    ArgumentCaptor<LockedDomainState> captured = ArgumentCaptor.forClass(LockedDomainState.class);
    Mockito.verify(check).accept(captured.capture());
    Assertions.assertEquals("Markets", captured.getValue().domain().parent().get());
    Mockito.verify(additional).accept(captured.getValue());
    Mockito.clearInvocations(check, additional);
    domainManager.detachDomain(METALAKE, "Trading");
    Mockito.verifyNoInteractions(check, additional);
  }

  /** Verifies a caller check can abort a mutation while the raw manager remains usable. */
  @Test
  public void testScopedMutationCheckFailureDoesNotAffectRawManager() {
    DomainDispatcher checked =
        domainManager.withMutationCheck(
            state -> {
              throw new IllegalStateException("Rejected by caller");
            });
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> checked.createDomain(METALAKE, "Finance", null, null, BACKING_TAG, false));
    Assertions.assertDoesNotThrow(
        () -> domainManager.createDomain(METALAKE, "Finance", null, null, BACKING_TAG, false));
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
        () -> domainManager.createDomain("missing", "Finance", null, null, BACKING_TAG, false));
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () ->
            domainManager.createSubdomain(
                "missing", "Finance", "Trading", null, null, BACKING_TAG, false));
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
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.updateBackingTag("missing", "Finance", BACKING_TAG));
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
    DomainEntity created =
        domainManager.createDomain(METALAKE, "Finance", "comment", null, BACKING_TAG, false);
    Mockito.verify(domainOperations)
        .insertDomainWithBackingTag(domainCaptor.capture(), eq(BACKING_TAG), checkCaptor.capture());

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
        .insertDomainWithBackingTag(any(), any(), any());
    Assertions.assertThrows(
        DomainAlreadyExistsException.class,
        () -> domainManager.createDomain(METALAKE, "Finance", null, null, BACKING_TAG, false));

    // The metalake was dropped after it was checked.
    Mockito.doThrow(new NoSuchEntityException("missing"))
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.createDomain(METALAKE, "Finance", null, null, BACKING_TAG, false));

    Mockito.doThrow(new IOException("insert failed"))
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    assertIOExceptionWrapped(
        () -> domainManager.createDomain(METALAKE, "Finance", null, null, BACKING_TAG, false));
  }

  @Test
  public void testCreateSubdomain() throws IOException {
    ArgumentCaptor<DomainEntity> domainCaptor = ArgumentCaptor.forClass(DomainEntity.class);
    ArgumentCaptor<Consumer<LockedDomainState>> checkCaptor = checkCaptor();
    domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null, BACKING_TAG, false);
    Mockito.verify(domainOperations)
        .insertDomainWithBackingTag(domainCaptor.capture(), eq(BACKING_TAG), checkCaptor.capture());
    Assertions.assertEquals(TRADING, domainCaptor.getValue().nameIdentifier());
    Assertions.assertEquals("Finance", domainCaptor.getValue().parent().get());
    assertIsNewDomainCheck(checkCaptor.getValue());

    // The sub-domain uses the parent name read under the parent's lock.
    Mockito.doAnswer(
            invocation -> {
              DomainEntity newDomain = invocation.getArgument(0);
              Consumer<LockedDomainState> check = invocation.getArgument(2);
              DomainEntity storedParent = domain("Finance", null, Domain.Status.DRAFT);
              check.accept(boundState(newDomain, storedParent));
              return null;
            })
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainManager.createSubdomain(
                METALAKE, "Finance ", "Desk", null, null, BACKING_TAG, false));
    DomainEntity desk =
        domainManager.createSubdomain(METALAKE, "Finance", "Desk", null, null, BACKING_TAG, false);
    Assertions.assertEquals("Finance", desk.parent().get());
    Assertions.assertEquals(NameIdentifierUtil.ofDomain(METALAKE, "Desk"), desk.nameIdentifier());

    // A check that throws reaches the caller unchanged.
    Mockito.doAnswer(TestDomainManagerWithMockStore::runInsertCheck)
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainManager.createSubdomain(
                METALAKE, "Trading", "Desk", null, null, BACKING_TAG, false));

    Mockito.doThrow(new NoSuchEntityException("missing"))
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () ->
            domainManager.createSubdomain(
                METALAKE, "missing", "Trading", null, null, BACKING_TAG, false));

    Mockito.doThrow(new EntityAlreadyExistsException("exists"))
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    Assertions.assertThrows(
        DomainAlreadyExistsException.class,
        () ->
            domainManager.createSubdomain(
                METALAKE, "Finance", "Trading", null, null, BACKING_TAG, false));

    Mockito.doThrow(new IOException("insert failed"))
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    assertIOExceptionWrapped(
        () ->
            domainManager.createSubdomain(
                METALAKE, "Finance", "Trading", null, null, BACKING_TAG, false));

    // A domain as its own parent, and a null parent, are refused before the store is called.
    Mockito.clearInvocations(domainOperations);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainManager.createSubdomain(
                METALAKE, "Finance", "Finance", null, null, BACKING_TAG, false));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainManager.createSubdomain(
                METALAKE, null, "Trading", null, null, BACKING_TAG, false));
    Mockito.verifyNoInteractions(domainOperations);
  }

  @Test
  public void testCreateDomainWithNewBackingTag() throws IOException {
    ArgumentCaptor<DomainEntity> domainCaptor = ArgumentCaptor.forClass(DomainEntity.class);
    ArgumentCaptor<TagEntity> tagCaptor = ArgumentCaptor.forClass(TagEntity.class);
    ArgumentCaptor<Consumer<LockedDomainState>> checkCaptor = checkCaptor();
    domainManager.createSubdomain(
        METALAKE, "Finance", "Trading", null, null, TagValue.noValue("trading_tag"), true);
    Mockito.verify(domainOperations)
        .insertDomainWithNewBackingTag(
            domainCaptor.capture(), tagCaptor.capture(), checkCaptor.capture());
    Assertions.assertEquals(TRADING, domainCaptor.getValue().nameIdentifier());
    TagEntity tag = tagCaptor.getValue();
    Assertions.assertEquals(
        NameIdentifierUtil.ofTag(METALAKE, "trading_tag"), tag.nameIdentifier());
    Assertions.assertEquals(100L, tag.id());
    Assertions.assertEquals(TagValueConstraint.noValue(), tag.valueConstraint());
    assertIsNewDomainCheck(checkCaptor.getValue());

    // A tag name in use reaches the caller unchanged.
    Mockito.doThrow(new TagAlreadyExistsException("exists"))
        .when(domainOperations)
        .insertDomainWithNewBackingTag(any(), any(), any());
    Assertions.assertThrows(
        TagAlreadyExistsException.class,
        () ->
            domainManager.createDomain(
                METALAKE, "Finance", null, null, TagValue.noValue("finance_tag"), true));

    // A missing tag, a value for a new tag and a null tag are refused before the store is called.
    Mockito.doThrow(new NoSuchTagException("missing"))
        .when(domainOperations)
        .insertDomainWithBackingTag(any(), any(), any());
    Assertions.assertThrows(
        NoSuchTagException.class,
        () ->
            domainManager.createDomain(
                METALAKE, "Finance", null, null, TagValue.noValue("missing"), false));
    Mockito.clearInvocations(domainOperations);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainManager.createDomain(
                METALAKE, "Finance", null, null, TagValue.of("finance_tag", "v"), true));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.createDomain(METALAKE, "Finance", null, null, null, false));
    Mockito.verifyNoInteractions(domainOperations);
  }

  @Test
  public void testUpdateBackingTag() throws IOException {
    ArgumentCaptor<Consumer<LockedDomainState>> checkCaptor = checkCaptor();
    TagValue backingTag = TagValue.of("dept", "finance");
    domainManager.updateBackingTag(METALAKE, "Finance", backingTag);
    Mockito.verify(domainOperations)
        .updateDomainBackingTagAndGet(eq(FINANCE), eq(backingTag), any(), checkCaptor.capture());
    assertIsBackingTagUpdateCheck(checkCaptor.getValue());

    Mockito.when(domainOperations.updateDomainBackingTagAndGet(eq(FINANCE), any(), any(), any()))
        .thenThrow(new NoSuchEntityException("missing"))
        .thenThrow(new NoSuchTagException("missing"))
        .thenThrow(new IOException("update failed"));
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.updateBackingTag(METALAKE, "Finance", null));
    Assertions.assertThrows(
        NoSuchTagException.class,
        () -> domainManager.updateBackingTag(METALAKE, "Finance", backingTag));
    assertIOExceptionWrapped(() -> domainManager.updateBackingTag(METALAKE, "Finance", null));
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
  public void testAlterDomainWithNullProperties() throws IOException {
    DomainEntity stored =
        domain("Finance", null, Domain.Status.DRAFT).toBuilder().withProperties(null).build();
    Mockito.when(entityStore.update(eq(FINANCE), eq(DomainEntity.class), any(), any()))
        .thenAnswer(
            invocation -> {
              Function<DomainEntity, DomainEntity> updater = invocation.getArgument(3);
              return updater.apply(stored);
            });

    DomainEntity updated =
        domainManager.alterDomain(METALAKE, "Finance", DomainChange.updateComment("new comment"));
    Assertions.assertEquals("new comment", updated.comment());
    Assertions.assertTrue(updated.properties().isEmpty());
    Assertions.assertEquals(
        Map.of("key", "value"),
        domainManager
            .alterDomain(METALAKE, "Finance", DomainChange.setProperty("key", "value"))
            .properties());
    Assertions.assertTrue(
        domainManager
            .alterDomain(METALAKE, "Finance", DomainChange.removeProperty("key"))
            .properties()
            .isEmpty());
    Assertions.assertNull(stored.properties());
  }

  @Test
  public void testAlterAndDeletePropagateOptimisticLockException() throws IOException {
    OptimisticLockException conflict = new OptimisticLockException("Concurrent operation");
    Mockito.when(entityStore.update(eq(FINANCE), eq(DomainEntity.class), any(), any()))
        .thenThrow(conflict);
    Mockito.when(domainOperations.deleteDomain(eq(FINANCE), any())).thenThrow(conflict);

    Assertions.assertSame(
        conflict,
        Assertions.assertThrows(
            OptimisticLockException.class,
            () -> domainManager.alterDomain(METALAKE, "Finance", DomainChange.updateComment("c"))));
    Assertions.assertSame(
        conflict,
        Assertions.assertThrows(
            OptimisticLockException.class, () -> domainManager.deleteDomain(METALAKE, "Finance")));
  }

  @Test
  public void testDeleteDomain() throws IOException {
    Mockito.when(domainOperations.deleteDomain(eq(FINANCE), any()))
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

  /** Verifies authorized snapshots cannot be replaced by a subsequent name lookup. */
  @Test
  public void testMemberListingUsesSuppliedSnapshotsWithoutReloading() throws IOException {
    DomainEntity published = domain("Finance", null, Domain.Status.PUBLISHED);
    DomainEntity replacement =
        published.toBuilder().withId(200L).withStatus(Domain.Status.DRAFT).build();
    Mockito.when(entityStore.get(FINANCE, Entity.EntityType.DOMAIN, DomainEntity.class))
        .thenReturn(replacement);

    Assertions.assertEquals(
        0,
        domainManager.listMembers(
                METALAKE, published, new DomainEntity[0], DomainMemberFilter.builder().build())
            .length);

    Mockito.verify(entityStore, Mockito.never())
        .get(FINANCE, Entity.EntityType.DOMAIN, DomainEntity.class);
    Mockito.verifyNoInteractions(domainOperations);
  }

  /** Verifies that member listing reads only the queried domain and its sub-domains. */
  @Test
  public void testMemberListingDoesNotScanMetalakeDomains() throws IOException {
    DomainEntity finance = domain("Finance", null, Domain.Status.DRAFT);
    Mockito.when(entityStore.get(FINANCE, Entity.EntityType.DOMAIN, DomainEntity.class))
        .thenReturn(finance);
    Mockito.when(domainOperations.listSubDomains(FINANCE)).thenReturn(Collections.emptyList());
    Assertions.assertEquals(
        0,
        domainManager.listMembers(METALAKE, "Finance", DomainMemberFilter.builder().build())
            .length);
    Mockito.verify(domainOperations).listSubDomains(FINANCE);
    Mockito.verify(entityStore, Mockito.never())
        .list(any(), eq(DomainEntity.class), eq(Entity.EntityType.DOMAIN));
    DomainEntity trading = domain("Trading", "Finance", Domain.Status.DRAFT);
    Mockito.when(entityStore.get(TRADING, Entity.EntityType.DOMAIN, DomainEntity.class))
        .thenReturn(trading);
    Assertions.assertEquals(
        0,
        domainManager.listMembers(METALAKE, "Trading", DomainMemberFilter.builder().build())
            .length);
    Mockito.verify(domainOperations, Mockito.never()).listSubDomains(TRADING);
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
    check.accept(boundState(desk, domain("Trading", null, Domain.Status.DRAFT)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> check.accept(boundState(desk, domain("Trading", "Finance", Domain.Status.DRAFT))));
  }

  // The backing tag update check refuses a value bound to another domain.
  private static void assertIsBackingTagUpdateCheck(Consumer<LockedDomainState> check) {
    DomainEntity finance = domain("Finance", null, Domain.Status.DRAFT);
    DomainEntity markets =
        domain("Markets", null, Domain.Status.DRAFT).toBuilder()
            .withBackingTagId(tag("dept").id())
            .withBackingTagValue("finance")
            .build();
    check.accept(
        new LockedDomainState(
            finance, null, Collections.emptyList(), tag("dept"), Collections.emptyList()));
    Assertions.assertThrows(
        BackingTagAlreadyBoundException.class,
        () ->
            check.accept(
                new LockedDomainState(
                    finance, null, Collections.emptyList(), tag("dept"), List.of(markets))));
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

  // Runs the check of an insert as the store would for a top-level domain bound to a tag that
  // allows
  // any value and backs no other domain.
  private static Object runTopLevelInsertCheck(InvocationOnMock invocation) {
    DomainEntity domain = invocation.getArgument(0);
    Consumer<LockedDomainState> check = invocation.getArgument(2);
    check.accept(boundState(domain, null));
    return null;
  }

  private static Object runInsertCheck(InvocationOnMock invocation) {
    DomainEntity domain = invocation.getArgument(0);
    Consumer<LockedDomainState> check = invocation.getArgument(2);
    // The parent of the new domain is itself a sub-domain.
    check.accept(
        boundState(domain, domain(domain.parent().get(), "Finance", Domain.Status.PUBLISHED)));
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

  // The locked state of a new domain bound to a tag that backs no other domain.
  private static LockedDomainState boundState(DomainEntity domain, DomainEntity parent) {
    return new LockedDomainState(
        domain, parent, Collections.emptyList(), tag("dept"), Collections.emptyList());
  }

  private static TagEntity tag(String name) {
    return TagEntity.builder()
        .withId((long) name.hashCode())
        .withName(name)
        .withNamespace(NamespaceUtil.ofTag(METALAKE))
        .withAuditInfo(auditInfo())
        .build();
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
