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
package org.apache.gravitino.storage.relational.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestDomainMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_domain_meta_service_test";

  private final Map<String, String> props = ImmutableMap.of("k1", "v1");

  @TestTemplate
  public void testInsertGetAndListDomains() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainMetaService.getDomainByIdentifier(
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing")));
    Assertions.assertTrue(
        domainMetaService.listDomainsByNamespace(NamespaceUtil.ofDomain(METALAKE_NAME)).isEmpty());

    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "finance comment");
    // A domain name may contain "/".
    DomainEntity trading = newDomain(METALAKE_NAME, "Finance/Trading", null);
    domainMetaService.insertDomain(finance, false);
    domainMetaService.insertDomain(trading, false);

    DomainEntity loaded = domainMetaService.getDomainByIdentifier(finance.nameIdentifier());
    Assertions.assertEquals(finance, loaded);
    Assertions.assertEquals(Domain.Status.DRAFT, loaded.status());
    Assertions.assertEquals(props, loaded.properties());
    Assertions.assertFalse(loaded.parent().isPresent());
    Assertions.assertFalse(loaded.backingTag().isPresent());
    Assertions.assertEquals(
        trading, domainMetaService.getDomainByIdentifier(trading.nameIdentifier()));
    Assertions.assertEquals(
        finance, backend.get(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertTrue(backend.exists(trading.nameIdentifier(), Entity.EntityType.DOMAIN));

    List<DomainEntity> domains =
        backend.list(NamespaceUtil.ofDomain(METALAKE_NAME), Entity.EntityType.DOMAIN, true);
    Assertions.assertEquals(2, domains.size());
    Assertions.assertTrue(domains.contains(finance));
    Assertions.assertTrue(domains.contains(trading));

    List<DomainEntity> batch =
        domainMetaService.batchGetDomainByIdentifier(
            Lists.newArrayList(
                finance.nameIdentifier(), NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing")));
    Assertions.assertEquals(Lists.newArrayList(finance), batch);
    Assertions.assertTrue(
        domainMetaService.batchGetDomainByIdentifier(Lists.newArrayList()).isEmpty());

    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            domainMetaService.insertDomain(
                newDomain(METALAKE_NAME, "Finance", "another finance"), false));
  }

  @TestTemplate
  public void testDomainNamesAreScopedByMetalake() throws IOException {
    String anotherMetalakeName = METALAKE_NAME + "_another";
    createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertMakeLake(anotherMetalakeName);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();

    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "comment");
    DomainEntity anotherFinance = newDomain(anotherMetalakeName, "Finance", "another comment");
    domainMetaService.insertDomain(finance, false);
    domainMetaService.insertDomain(anotherFinance, false);

    Assertions.assertEquals(
        finance, domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));
    Assertions.assertEquals(
        anotherFinance, domainMetaService.getDomainByIdentifier(anotherFinance.nameIdentifier()));
    Assertions.assertEquals(
        1, domainMetaService.listDomainsByNamespace(NamespaceUtil.ofDomain(METALAKE_NAME)).size());
  }

  @TestTemplate
  public void testInsertDomainWithOverwrite() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "comment");
    domainMetaService.insertDomain(finance, false);

    DomainEntity replacement =
        finance.toBuilder().withComment("replaced").withStatus(Domain.Status.PUBLISHED).build();
    domainMetaService.insertDomain(replacement, true);
    Assertions.assertEquals(
        replacement, domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));
    Assertions.assertEquals(2L, selectDomainPO(finance.id()).currentVersion());

    // An overwrite cannot reuse the ID of a deleted domain.
    Assertions.assertTrue(domainMetaService.deleteDomain(finance.nameIdentifier()));
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> domainMetaService.insertDomain(replacement, true));

    // An overwrite with no matching name or ID inserts a new domain.
    DomainEntity markets = newDomain(METALAKE_NAME, "Markets", null);
    domainMetaService.insertDomain(markets, true);
    Assertions.assertEquals(
        markets, domainMetaService.getDomainByIdentifier(markets.nameIdentifier()));
    Assertions.assertEquals(1L, selectDomainPO(markets.id()).currentVersion());
  }

  @TestTemplate
  public void testOverwriteMatchesDomainById() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    String anotherMetalakeName = METALAKE_NAME + "_another";
    createAndInsertMakeLake(anotherMetalakeName);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "comment");
    domainMetaService.insertDomain(finance, false);
    domainMetaService.updateDomain(
        finance.nameIdentifier(),
        entity -> ((DomainEntity) entity).toBuilder().withName("Finance/Tax").build());

    // The name is free after the rename, so the overwrite finds the domain by its ID and replaces
    // the renamed row instead of adding a second domain.
    DomainEntity replacement = finance.toBuilder().withComment("replaced").build();
    domainMetaService.insertDomain(replacement, true);
    Assertions.assertEquals(
        replacement, domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainMetaService.getDomainByIdentifier(
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "Finance/Tax")));
    Assertions.assertEquals(
        1, domainMetaService.listDomainsByNamespace(NamespaceUtil.ofDomain(METALAKE_NAME)).size());

    // The ID cannot be taken over by a domain of another metalake.
    DomainEntity elsewhere =
        finance.toBuilder().withNamespace(NamespaceUtil.ofDomain(anotherMetalakeName)).build();
    Assertions.assertThrows(
        EntityAlreadyExistsException.class, () -> domainMetaService.insertDomain(elsewhere, true));
    Assertions.assertTrue(
        domainMetaService
            .listDomainsByNamespace(NamespaceUtil.ofDomain(anotherMetalakeName))
            .isEmpty());
  }

  @TestTemplate
  public void testBackendReadsAndWritesDomains() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    DomainEntity markets = newDomain(METALAKE_NAME, "Markets", null);
    backend.insert(finance, false);
    backend.insert(markets, false);

    List<DomainEntity> batch =
        backend.batchGet(
            Lists.newArrayList(
                finance.nameIdentifier(),
                markets.nameIdentifier(),
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing")),
            Entity.EntityType.DOMAIN);
    Assertions.assertEquals(2, batch.size());
    Assertions.assertTrue(batch.contains(finance));
    Assertions.assertTrue(batch.contains(markets));

    DomainEntity updated =
        backend.<DomainEntity>update(
            finance.nameIdentifier(),
            Entity.EntityType.DOMAIN,
            entity -> entity.toBuilder().withComment("updated").build());
    Assertions.assertEquals("updated", updated.comment());
    Assertions.assertEquals(
        updated, backend.get(finance.nameIdentifier(), Entity.EntityType.DOMAIN));

    Assertions.assertTrue(
        backend.delete(finance.nameIdentifier(), Entity.EntityType.DOMAIN, false));
    Assertions.assertFalse(
        backend.delete(finance.nameIdentifier(), Entity.EntityType.DOMAIN, false));
    Assertions.assertFalse(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertTrue(backend.exists(markets.nameIdentifier(), Entity.EntityType.DOMAIN));
  }

  @TestTemplate
  public void testDomainIsLockableAsLiveEndpoint() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    domainMetaService.insertDomain(finance, false);
    NamespacedEntityId observed =
        EntityIdService.getEntityIds(finance.nameIdentifier(), Entity.EntityType.DOMAIN);

    SessionUtils.doMultipleWithCommit(
        () ->
            LiveEndpointService.lockLiveEndpoint(
                finance.nameIdentifier(), Entity.EntityType.DOMAIN, observed));

    // A domain recreated with the same name is a different endpoint.
    Assertions.assertTrue(domainMetaService.deleteDomain(finance.nameIdentifier()));
    domainMetaService.insertDomain(newDomain(METALAKE_NAME, "Finance", null), false);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    LiveEndpointService.lockLiveEndpoint(
                        finance.nameIdentifier(), Entity.EntityType.DOMAIN, observed)));
  }

  @TestTemplate
  public void testInsertDomainIntoMissingMetalake() {
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity domain = newDomain("metalake_that_does_not_exist", "Finance", null);

    Assertions.assertThrows(
        NoSuchEntityException.class, () -> domainMetaService.insertDomain(domain, false));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> domainMetaService.insertDomain(domain, true));
  }

  @TestTemplate
  public void testUpdateDomain() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "comment");
    DomainEntity markets = newDomain(METALAKE_NAME, "Markets", null);
    domainMetaService.insertDomain(finance, false);
    domainMetaService.insertDomain(markets, false);

    AuditInfo updatedAudit =
        AuditInfo.builder()
            .withCreator(AUDIT_INFO.creator())
            .withCreateTime(AUDIT_INFO.createTime())
            .withLastModifier("modifier")
            .withLastModifiedTime(Instant.now())
            .build();
    DomainEntity updated =
        domainMetaService.updateDomain(
            finance.nameIdentifier(),
            entity ->
                ((DomainEntity) entity)
                    .toBuilder()
                        .withName("Finance/Tax")
                        .withComment(null)
                        .withProperties(ImmutableMap.of("k2", "v2"))
                        .withAuditInfo(updatedAudit)
                        .build());

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));
    DomainEntity loaded =
        domainMetaService.getDomainByIdentifier(
            NameIdentifierUtil.ofDomain(METALAKE_NAME, "Finance/Tax"));
    Assertions.assertEquals(updated, loaded);
    Assertions.assertEquals(finance.id(), loaded.id());
    Assertions.assertNull(loaded.comment());
    Assertions.assertEquals(ImmutableMap.of("k2", "v2"), loaded.properties());
    Assertions.assertEquals("modifier", loaded.auditInfo().lastModifier());
    Assertions.assertEquals(2L, selectDomainPO(finance.id()).currentVersion());

    // The status is changed only with the status locks, not by an update.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainMetaService.updateDomain(
                loaded.nameIdentifier(),
                entity ->
                    ((DomainEntity) entity)
                        .toBuilder().withStatus(Domain.Status.PUBLISHED).build()));
    Assertions.assertEquals(2L, selectDomainPO(finance.id()).currentVersion());

    // Renaming to a name in use fails.
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            domainMetaService.updateDomain(
                loaded.nameIdentifier(),
                entity -> ((DomainEntity) entity).toBuilder().withName("Markets").build()));

    // The updater must keep the domain ID.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainMetaService.updateDomain(
                markets.nameIdentifier(),
                entity ->
                    ((DomainEntity) entity)
                        .toBuilder().withId(RandomIdGenerator.INSTANCE.nextId()).build()));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainMetaService.updateDomain(
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing"), entity -> entity));
  }

  @TestTemplate
  public void testAlterWaitsForConcurrentAlterAndKeepsItsChange() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "initial");
    domainMetaService.insertDomain(finance, false);

    CountDownLatch alterWritten = new CountDownLatch(1);
    CountDownLatch allowAlterCommit = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Throwable> competingAlter =
          alterCommentInOpenTransaction(
              executor, finance.id(), "competing", alterWritten, allowAlterCommit);
      Assertions.assertTrue(alterWritten.await(30, TimeUnit.SECONDS));

      // This alter reads the domain before the competing alter commits, then waits for its lock.
      DomainEntity[] givenToUpdater = new DomainEntity[1];
      Future<DomainEntity> alter =
          executor.submit(
              () ->
                  domainMetaService.updateDomain(
                      finance.nameIdentifier(),
                      entity -> {
                        givenToUpdater[0] = (DomainEntity) entity;
                        return givenToUpdater[0].toBuilder()
                            .withProperties(ImmutableMap.of("k2", "v2"))
                            .build();
                      }));
      Assertions.assertThrows(TimeoutException.class, () -> alter.get(500, TimeUnit.MILLISECONDS));

      allowAlterCommit.countDown();
      Assertions.assertNull(competingAlter.get(30, TimeUnit.SECONDS));
      DomainEntity altered = alter.get(30, TimeUnit.SECONDS);
      // The updater ran on the locked row, so both changes are kept.
      Assertions.assertEquals("competing", givenToUpdater[0].comment());
      Assertions.assertEquals("competing", altered.comment());
      Assertions.assertEquals(ImmutableMap.of("k2", "v2"), altered.properties());
      Assertions.assertEquals(
          altered, domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));
    } finally {
      allowAlterCommit.countDown();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testDeleteWaitsForConcurrentAlterAndDeletes() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "initial");
    domainMetaService.insertDomain(finance, false);

    CountDownLatch alterWritten = new CountDownLatch(1);
    CountDownLatch allowAlterCommit = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Throwable> competingAlter =
          alterCommentInOpenTransaction(
              executor, finance.id(), "competing", alterWritten, allowAlterCommit);
      Assertions.assertTrue(alterWritten.await(30, TimeUnit.SECONDS));

      // This delete reads the domain before the alter commits, then waits for its lock.
      Future<Boolean> delete =
          executor.submit(() -> domainMetaService.deleteDomain(finance.nameIdentifier()));
      Assertions.assertThrows(TimeoutException.class, () -> delete.get(500, TimeUnit.MILLISECONDS));

      allowAlterCommit.countDown();
      Assertions.assertNull(competingAlter.get(30, TimeUnit.SECONDS));
      Assertions.assertTrue(delete.get(30, TimeUnit.SECONDS));
      Assertions.assertFalse(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    } finally {
      allowAlterCommit.countDown();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testDeleteDomainCleansDependentRelations() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    TagEntity tag = newTag("Finance");
    backend.insert(tag, false);
    DomainEntity finance =
        newDomain(METALAKE_NAME, "Finance", null).toBuilder().withBackingTagId(tag.id()).build();
    domainMetaService.insertDomain(finance, false);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "domain_owner",
            AUDIT_INFO);
    backend.insert(user, false);
    OwnerMetaService.getInstance()
        .setOwner(
            finance.nameIdentifier(), Entity.EntityType.DOMAIN, user.nameIdentifier(), user.type());

    insertPrivilegeOnDomain(finance.id());

    String domainAsMetadataObject =
        String.format("metadata_object_id = %d AND metadata_object_type = 'DOMAIN'", finance.id());
    String domainAsSecurableObject =
        String.format("metadata_object_id = %d AND type = 'DOMAIN'", finance.id());
    Assertions.assertEquals(1, countActiveRows("owner_meta", domainAsMetadataObject));
    Assertions.assertEquals(
        1, countActiveRows("role_meta_securable_object", domainAsSecurableObject));

    // A delete that observed the domain before a rename no longer names the row: it reports the
    // domain as missing and cleans nothing.
    DomainPO preRenamePO = selectDomainPO(finance.id());
    DomainEntity treasury =
        domainMetaService.updateDomain(
            finance.nameIdentifier(),
            entity -> ((DomainEntity) entity).toBuilder().withName("Treasury").build());
    Assertions.assertFalse(domainMetaService.deleteDomain(finance.nameIdentifier(), preRenamePO));
    Assertions.assertEquals(1, countActiveRows("owner_meta", domainAsMetadataObject));
    Assertions.assertEquals(
        1, countActiveRows("role_meta_securable_object", domainAsSecurableObject));

    // A delete that observed an older version of the same domain locks the row and deletes it.
    DomainPO stalePO = selectDomainPO(treasury.id());
    domainMetaService.updateDomain(
        treasury.nameIdentifier(),
        entity -> ((DomainEntity) entity).toBuilder().withComment("updated").build());
    Assertions.assertTrue(domainMetaService.deleteDomain(treasury.nameIdentifier(), stalePO));
    Assertions.assertFalse(domainMetaService.deleteDomain(treasury.nameIdentifier()));
    Assertions.assertFalse(backend.exists(treasury.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertEquals(0, countActiveRows("owner_meta", domainAsMetadataObject));
    Assertions.assertEquals(
        0, countActiveRows("role_meta_securable_object", domainAsSecurableObject));
    // The backing tag is kept.
    Assertions.assertTrue(backend.exists(tag.nameIdentifier(), Entity.EntityType.TAG));

    // The deleted domain row is a stale observation now.
    Assertions.assertFalse(domainMetaService.deleteDomain(treasury.nameIdentifier(), stalePO));
  }

  @TestTemplate
  public void testBackingTagIsStoredOnDomainRow() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    TagEntity dept = newTag("dept");
    TagEntity finance = newTag("Finance");
    backend.insert(dept, false);
    backend.insert(finance, false);

    DomainEntity trading =
        newDomain(METALAKE_NAME, "Trading", null).toBuilder()
            .withBackingTagId(dept.id())
            .withBackingTagValue("trading")
            .build();
    DomainEntity financeDomain =
        newDomain(METALAKE_NAME, "Finance", null).toBuilder()
            .withBackingTagId(finance.id())
            .build();
    domainMetaService.insertDomain(trading, false);
    domainMetaService.insertDomain(financeDomain, false);

    DomainEntity loaded = domainMetaService.getDomainByIdentifier(trading.nameIdentifier());
    Assertions.assertEquals(TagValue.of("dept", "trading"), loaded.backingTag().orElse(null));
    Assertions.assertEquals(dept.id(), loaded.backingTagId());
    Assertions.assertEquals(
        TagValue.noValue("Finance"),
        domainMetaService
            .getDomainByIdentifier(financeDomain.nameIdentifier())
            .backingTag()
            .orElse(null));
    Assertions.assertTrue(
        domainMetaService.listDomainsByNamespace(NamespaceUtil.ofDomain(METALAKE_NAME)).stream()
            .allMatch(domain -> domain.backingTag().isPresent()));

    // The binding is by tag ID, so a renamed tag keeps backing the domain under its new name.
    TagMetaService.getInstance()
        .updateTag(
            dept.nameIdentifier(),
            entity ->
                TagEntity.builder()
                    .withId(dept.id())
                    .withName("department")
                    .withNamespace(dept.namespace())
                    .withAuditInfo(AUDIT_INFO)
                    .build());
    Assertions.assertEquals(
        TagValue.of("department", "trading"),
        domainMetaService
            .getDomainByIdentifier(trading.nameIdentifier())
            .backingTag()
            .orElse(null));

    // A deleted tag leaves its ID on the domain, which then has no backing tag, and a new tag with
    // the same name does not take its place.
    Assertions.assertTrue(
        TagMetaService.getInstance()
            .deleteTag(NameIdentifierUtil.ofTag(METALAKE_NAME, "department")));
    backend.insert(newTag("department"), false);
    DomainEntity orphaned = domainMetaService.getDomainByIdentifier(trading.nameIdentifier());
    Assertions.assertFalse(orphaned.backingTag().isPresent());
    Assertions.assertEquals(dept.id(), orphaned.backingTagId());
    Assertions.assertEquals("trading", orphaned.backingTagValue());

    // An update writes the binding: replace it, then remove it.
    DomainEntity rebound =
        domainMetaService.updateDomain(
            trading.nameIdentifier(),
            entity ->
                ((DomainEntity) entity)
                    .toBuilder().withBackingTagId(finance.id()).withBackingTagValue(null).build());
    Assertions.assertEquals(finance.id(), rebound.backingTagId());
    Assertions.assertEquals(TagValue.noValue("Finance"), rebound.backingTag().orElse(null));
    Assertions.assertEquals(
        TagValue.noValue("Finance"),
        domainMetaService
            .getDomainByIdentifier(trading.nameIdentifier())
            .backingTag()
            .orElse(null));

    domainMetaService.updateDomain(
        trading.nameIdentifier(),
        entity ->
            ((DomainEntity) entity)
                .toBuilder().withBackingTagId(null).withBackingTagValue(null).build());
    DomainEntity unbound = domainMetaService.getDomainByIdentifier(trading.nameIdentifier());
    Assertions.assertFalse(unbound.backingTag().isPresent());
    Assertions.assertNull(unbound.backingTagId());

    // Rebinding away from a live tag returns the new tag's name, not the one the updater's entity
    // still carries.
    TagEntity region = newTag("region");
    backend.insert(region, false);
    DomainEntity switched =
        domainMetaService.updateDomain(
            financeDomain.nameIdentifier(),
            entity ->
                ((DomainEntity) entity)
                    .toBuilder().withBackingTagId(region.id()).withBackingTagValue("apac").build());
    Assertions.assertEquals(TagValue.of("region", "apac"), switched.backingTag().orElse(null));
    Assertions.assertEquals(
        switched, domainMetaService.getDomainByIdentifier(financeDomain.nameIdentifier()));
  }

  @TestTemplate
  public void testDeleteDomainWaitsForConcurrentMetalakeDelete() throws Exception {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    domainMetaService.insertDomain(finance, false);
    MetalakePO observedMetalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalake.name()));

    // The metalake drop holds its exclusive row lock, so the domain delete must wait for it
    // instead of racing it on the domain and privilege rows.
    CountDownLatch dropWritten = new CountDownLatch(1);
    CountDownLatch allowDropCommit = new CountDownLatch(1);
    CountDownLatch deleteStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> dropResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        Assertions.assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                MetalakeMetaMapper.class,
                                mapper ->
                                    mapper.softDeleteMetalakeMetaByMetalakeId(
                                        observedMetalakePO.getMetalakeId(),
                                        observedMetalakePO.getCurrentVersion()))),
                    () -> {
                      dropWritten.countDown();
                      await(allowDropCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      Assertions.assertTrue(dropWritten.await(30, TimeUnit.SECONDS));
      Future<Boolean> deleteResult =
          executor.submit(
              () -> {
                deleteStarted.countDown();
                return domainMetaService.deleteDomain(finance.nameIdentifier());
              });
      Assertions.assertTrue(deleteStarted.await(30, TimeUnit.SECONDS));
      Assertions.assertThrows(
          TimeoutException.class, () -> deleteResult.get(500, TimeUnit.MILLISECONDS));

      allowDropCommit.countDown();
      Assertions.assertNull(dropResult.get(30, TimeUnit.SECONDS));
      // The metalake is gone once the delete gets the lock, so the domain is reported as missing.
      Assertions.assertFalse(deleteResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowDropCommit.countDown();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testAlterHoldsMetalakeFenceAgainstCascadeDrop() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    TagEntity tag = newTag("Finance");
    backend.insert(tag, false);
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    domainMetaService.insertDomain(finance, false);

    CountDownLatch domainLocked = new CountDownLatch(1);
    CountDownLatch allowTagLock = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // Like a backing tag change, the updater locks the tag row while the domain row is locked.
      Future<DomainEntity> alter =
          executor.submit(
              () ->
                  domainMetaService.updateDomain(
                      finance.nameIdentifier(),
                      entity -> {
                        domainLocked.countDown();
                        await(allowTagLock);
                        SessionUtils.getWithoutCommit(
                            TagMetaMapper.class,
                            mapper -> mapper.selectTagByTagIdForUpdate(tag.id()));
                        return ((DomainEntity) entity)
                            .toBuilder().withBackingTagId(tag.id()).build();
                      }));
      Assertions.assertTrue(domainLocked.await(30, TimeUnit.SECONDS));

      // A cascade drop locks tags before domains. It must wait at the metalake fence instead of
      // taking the tag row the alter is about to lock.
      Future<Boolean> drop =
          executor.submit(
              () ->
                  MetalakeMetaService.getInstance()
                      .deleteMetalake(NameIdentifierUtil.ofMetalake(METALAKE_NAME), true));
      Assertions.assertThrows(TimeoutException.class, () -> drop.get(500, TimeUnit.MILLISECONDS));

      allowTagLock.countDown();
      Assertions.assertEquals(tag.id(), alter.get(30, TimeUnit.SECONDS).backingTagId());
      Assertions.assertTrue(drop.get(30, TimeUnit.SECONDS));
    } finally {
      allowTagLock.countDown();
      executor.shutdownNow();
    }
    Assertions.assertFalse(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
  }

  @TestTemplate
  public void testOverwriteRejectsIdOfDeletedDomain() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    domainMetaService.insertDomain(finance, false);
    Assertions.assertTrue(domainMetaService.deleteDomain(finance.nameIdentifier()));

    // The deleted row still holds the ID until garbage collection, so the ID cannot be reused.
    EntityAlreadyExistsException exception =
        Assertions.assertThrows(
            EntityAlreadyExistsException.class,
            () -> domainMetaService.insertDomain(finance, true));
    Assertions.assertTrue(
        exception.getMessage().contains(String.valueOf(finance.id())), exception.getMessage());
    Assertions.assertFalse(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
  }

  @TestTemplate
  public void testOverwriteRacingInsertOfSameName() throws Exception {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity first = newDomain(METALAKE_NAME, "Finance", "first");
    DomainEntity second = newDomain(METALAKE_NAME, "Finance", "second");

    CountDownLatch firstWritten = new CountDownLatch(1);
    CountDownLatch allowFirstCommit = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Throwable overwriteFailure;
    try {
      Future<Throwable> firstInsert =
          executor.submit(
              () -> {
                try {
                  SessionUtils.doMultipleWithCommit(
                      () ->
                          SessionUtils.doWithoutCommit(
                              DomainMetaMapper.class,
                              mapper ->
                                  mapper.insertDomainMeta(
                                      DomainPO.initializeDomainPO(first, metalake.id()))),
                      () -> {
                        firstWritten.countDown();
                        await(allowFirstCommit);
                      });
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      Assertions.assertTrue(firstWritten.await(30, TimeUnit.SECONDS));

      Future<Throwable> overwrite =
          executor.submit(
              () -> {
                try {
                  domainMetaService.insertDomain(second, true);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      Assertions.assertThrows(
          TimeoutException.class, () -> overwrite.get(500, TimeUnit.MILLISECONDS));

      allowFirstCommit.countDown();
      Assertions.assertNull(firstInsert.get(30, TimeUnit.SECONDS));
      overwriteFailure = overwrite.get(30, TimeUnit.SECONDS);
    } finally {
      allowFirstCommit.countDown();
      executor.shutdownNow();
    }

    // Where the locking read waits for the uncommitted row, the overwrite replaces it. Where the
    // read misses it, the insert hits the unique key, and the overwrite is reported as a
    // concurrent modification rather than as an existing domain.
    DomainEntity stored = domainMetaService.getDomainByIdentifier(first.nameIdentifier());
    if (overwriteFailure == null) {
      Assertions.assertEquals("second", stored.comment());
    } else {
      Assertions.assertTrue(
          overwriteFailure instanceof OptimisticLockException, String.valueOf(overwriteFailure));
      Assertions.assertEquals(first, stored);
    }
    Assertions.assertEquals(
        1, domainMetaService.listDomainsByNamespace(NamespaceUtil.ofDomain(METALAKE_NAME)).size());
  }

  @TestTemplate
  public void testDomainLifeCycleWithMetalakeDeleteAndGarbageCollection() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    String anotherMetalakeName = METALAKE_NAME + "_another";
    createAndInsertMakeLake(anotherMetalakeName);

    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    DomainEntity anotherFinance = newDomain(anotherMetalakeName, "Finance", null);
    backend.insert(finance, false);
    backend.insert(anotherFinance, false);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
    Assertions.assertFalse(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertTrue(
        backend.exists(anotherFinance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertTrue(legacyRecordExistsInDB(finance.id(), Entity.EntityType.DOMAIN));

    Assertions.assertEquals(0, backend.deleteOldVersionData(Entity.EntityType.DOMAIN, 1L));
    Assertions.assertTrue(
        backend.hardDeleteLegacyData(Entity.EntityType.DOMAIN, Instant.now().toEpochMilli() + 1000)
            >= 1);
    Assertions.assertFalse(legacyRecordExistsInDB(finance.id(), Entity.EntityType.DOMAIN));
    Assertions.assertTrue(
        backend.exists(anotherFinance.nameIdentifier(), Entity.EntityType.DOMAIN));

    // A cascading metalake delete removes its domains as well.
    BaseMetalake cascadeMetalake = createAndInsertMakeLake(METALAKE_NAME + "_cascade");
    DomainEntity markets = newDomain(cascadeMetalake.name(), "Markets", null);
    backend.insert(markets, false);
    UserEntity owner =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(cascadeMetalake.name()),
            "markets_owner",
            AUDIT_INFO);
    backend.insert(owner, false);
    OwnerMetaService.getInstance()
        .setOwner(
            markets.nameIdentifier(),
            Entity.EntityType.DOMAIN,
            owner.nameIdentifier(),
            owner.type());
    String marketsAsMetadataObject =
        String.format("metadata_object_id = %d AND metadata_object_type = 'DOMAIN'", markets.id());
    Assertions.assertEquals(1, countActiveRows("owner_meta", marketsAsMetadataObject));
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(cascadeMetalake.nameIdentifier(), true));
    Assertions.assertFalse(backend.exists(markets.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertEquals(0, countActiveRows("owner_meta", marketsAsMetadataObject));
  }

  @TestTemplate
  public void testDomainIdAndFullNameLookup() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    domainMetaService.insertDomain(finance, false);

    Assertions.assertEquals(
        finance.id(), domainMetaService.getDomainIdByMetalakeIdAndName(metalake.id(), "Finance"));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> domainMetaService.getDomainIdByMetalakeIdAndName(metalake.id(), "missing"));
    Assertions.assertEquals(
        finance.id(),
        EntityIdService.getEntityId(finance.nameIdentifier(), Entity.EntityType.DOMAIN));

    Map<Long, String> fullNames =
        MetadataObjectService.TYPE_TO_FULLNAME_FUNCTION_MAP
            .get(MetadataObject.Type.DOMAIN)
            .apply(Lists.newArrayList(finance.id(), RandomIdGenerator.INSTANCE.nextId()));
    Assertions.assertEquals(ImmutableMap.of(finance.id(), "Finance"), fullNames);
    Assertions.assertTrue(
        MetadataObjectService.TYPE_TO_FULLNAME_FUNCTION_MAP
            .get(MetadataObject.Type.DOMAIN)
            .apply(Lists.newArrayList())
            .isEmpty());
    Assertions.assertEquals(
        MetadataObjects.of(null, "Finance", MetadataObject.Type.DOMAIN),
        NameIdentifierUtil.toMetadataObject(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
  }

  /**
   * Alters the comment of a domain in a transaction that stays open, holding the row lock, until
   * {@code allowCommit} is released.
   */
  private Future<Throwable> alterCommentInOpenTransaction(
      ExecutorService executor,
      long domainId,
      String comment,
      CountDownLatch written,
      CountDownLatch allowCommit) {
    DomainPO observedPO = selectDomainPO(domainId);
    DomainEntity observed =
        DomainPO.fromDomainPO(observedPO, NamespaceUtil.ofDomain(METALAKE_NAME));
    DomainPO alteredPO =
        DomainPO.updateDomainPO(observedPO, observed.toBuilder().withComment(comment).build());
    return executor.submit(
        () -> {
          try {
            SessionUtils.doMultipleWithCommit(
                () ->
                    Assertions.assertEquals(
                        Integer.valueOf(1),
                        SessionUtils.getWithoutCommit(
                            DomainMetaMapper.class,
                            mapper -> mapper.updateDomainMeta(alteredPO, observedPO))),
                () -> {
                  written.countDown();
                  await(allowCommit);
                });
            return null;
          } catch (Throwable throwable) {
            return throwable;
          }
        });
  }

  private void await(CountDownLatch latch) {
    try {
      Assertions.assertTrue(latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private DomainEntity newDomain(String metalake, String name, String comment) {
    return DomainEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain(metalake))
        .withComment(comment)
        .withProperties(props)
        .withStatus(Domain.Status.DRAFT)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private TagEntity newTag(String name) {
    return TagEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private void insertPrivilegeOnDomain(long domainId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "INSERT INTO role_meta_securable_object (role_id, metadata_object_id, type,"
              + " privilege_names, privilege_conditions, current_version, last_version,"
              + " deleted_at) VALUES ("
              + RandomIdGenerator.INSTANCE.nextId()
              + ", "
              + domainId
              + ", 'DOMAIN', 'USE_DOMAIN', 'ALLOW', 1, 1, 0)");
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  private DomainPO selectDomainPO(long domainId) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class, mapper -> mapper.selectDomainMetaById(domainId));
  }

  private int countActiveRows(String table, String whereClause) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM %s WHERE %s AND deleted_at = 0", table, whereClause))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("Doesn't contain data");
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }
}
