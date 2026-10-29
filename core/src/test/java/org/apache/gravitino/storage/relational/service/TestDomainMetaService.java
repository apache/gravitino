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
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
import org.apache.gravitino.storage.relational.po.DomainPO;
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

    DomainEntity published =
        backend.<DomainEntity>update(
            finance.nameIdentifier(),
            Entity.EntityType.DOMAIN,
            entity -> entity.toBuilder().withStatus(Domain.Status.PUBLISHED).build());
    Assertions.assertEquals(Domain.Status.PUBLISHED, published.status());
    Assertions.assertEquals(
        published, backend.get(finance.nameIdentifier(), Entity.EntityType.DOMAIN));

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
                        .withStatus(Domain.Status.PUBLISHED)
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
    Assertions.assertEquals(Domain.Status.PUBLISHED, loaded.status());
    Assertions.assertEquals("modifier", loaded.auditInfo().lastModifier());
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
  public void testDomainAlterReportsOptimisticLockConflict() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainMetaService domainMetaService = DomainMetaService.getInstance();
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", "initial");
    domainMetaService.insertDomain(finance, false);

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            domainMetaService.updateDomain(
                finance.nameIdentifier(),
                entity -> {
                  DomainEntity current = (DomainEntity) entity;
                  // Another writer changes the domain after this update read it.
                  try {
                    domainMetaService.insertDomain(
                        current.toBuilder().withComment("competing").build(), true);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                  return current.toBuilder().withComment("requested").build();
                }));
    Assertions.assertEquals(
        "competing", domainMetaService.getDomainByIdentifier(finance.nameIdentifier()).comment());
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

    // A delete that observed an older version is rejected and cleans nothing.
    DomainPO stalePO = selectDomainPO(finance.id());
    domainMetaService.updateDomain(
        finance.nameIdentifier(),
        entity -> ((DomainEntity) entity).toBuilder().withComment("updated").build());
    Assertions.assertThrows(
        OptimisticLockException.class,
        () -> domainMetaService.deleteDomain(finance.nameIdentifier(), stalePO));
    Assertions.assertEquals(1, countActiveRows("owner_meta", domainAsMetadataObject));
    Assertions.assertEquals(
        1, countActiveRows("role_meta_securable_object", domainAsSecurableObject));

    Assertions.assertTrue(domainMetaService.deleteDomain(finance.nameIdentifier()));
    Assertions.assertFalse(domainMetaService.deleteDomain(finance.nameIdentifier()));
    Assertions.assertFalse(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertEquals(0, countActiveRows("owner_meta", domainAsMetadataObject));
    Assertions.assertEquals(
        0, countActiveRows("role_meta_securable_object", domainAsSecurableObject));
    // The backing tag is kept.
    Assertions.assertTrue(backend.exists(tag.nameIdentifier(), Entity.EntityType.TAG));

    // The deleted domain row is a stale observation now.
    Assertions.assertFalse(domainMetaService.deleteDomain(finance.nameIdentifier(), stalePO));
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
