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

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_CHANGE_LOG_POLL_BATCH_SIZE;
import static org.apache.gravitino.Configs.DEFAULT_ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES;
import static org.apache.gravitino.Configs.DEFAULT_ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS;
import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_POLL_BATCH_SIZE;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_RETENTION_SECS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_IDLE_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDomainManager {

  private static final String METALAKE = "metalake_for_domain_test";
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";

  private static EntityStore entityStore;
  private static DomainManager domainManager;

  @BeforeAll
  public static void setUp() throws IllegalAccessException, IOException {
    IdGenerator idGenerator = new RandomIdGenerator();
    Config config = mockConfig();
    entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);
    domainManager = new DomainManager(idGenerator, entityStore);

    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalDomainDispatcher", domainManager, true);

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE)
            .withVersion(SchemaVersion.V_0_1)
            .withComment("Test metalake")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    entityStore.put(metalake, false /* overwritten */);
  }

  @AfterAll
  public static void tearDown() throws IOException, IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "internalDomainDispatcher", null, true);
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
    FileUtils.deleteDirectory(new File(JDBC_STORE_PATH));
  }

  @AfterEach
  public void cleanUp() {
    DomainEntity[] domains = domainManager.listDomainInfos(METALAKE);
    // Sub-domains first, since a domain with sub-domains cannot be deleted.
    Arrays.stream(domains)
        .filter(domain -> domain.parent().isPresent())
        .forEach(domain -> domainManager.deleteDomain(METALAKE, domain.name()));
    Arrays.stream(domains)
        .filter(domain -> !domain.parent().isPresent())
        .forEach(domain -> domainManager.deleteDomain(METALAKE, domain.name()));
  }

  @Test
  public void testCreateGetAndListDomains() {
    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    DomainEntity finance = domainManager.createDomain(METALAKE, "Finance", "comment", properties);
    Assertions.assertEquals("Finance", finance.name());
    Assertions.assertEquals(NamespaceUtil.ofDomain(METALAKE), finance.namespace());
    Assertions.assertEquals("comment", finance.comment());
    Assertions.assertEquals(properties, finance.properties());
    Assertions.assertEquals(Domain.Status.DRAFT, finance.status());
    Assertions.assertFalse(finance.parent().isPresent());
    Assertions.assertFalse(finance.backingTag().isPresent());
    Assertions.assertNotNull(finance.auditInfo().creator());
    Assertions.assertEquals(finance, domainManager.getDomain(METALAKE, "Finance"));

    DomainEntity markets = domainManager.createDomain(METALAKE, "Markets", null, null);
    Assertions.assertNull(markets.comment());
    Assertions.assertTrue(markets.properties().isEmpty());

    Assertions.assertEquals(
        ImmutableSet.of("Finance", "Markets"),
        ImmutableSet.copyOf(domainManager.listDomains(METALAKE)));
    Assertions.assertEquals(
        ImmutableSet.of(finance, markets),
        ImmutableSet.copyOf(domainManager.listDomainInfos(METALAKE)));

    Assertions.assertThrows(
        DomainAlreadyExistsException.class,
        () -> domainManager.createDomain(METALAKE, "Finance", null, null));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.getDomain(METALAKE, "missing"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> domainManager.createDomain("missing_metalake", "Finance", null, null));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> domainManager.listDomains("missing_metalake"));
  }

  @Test
  public void testCreateSubdomain() {
    domainManager.createDomain(METALAKE, "Finance", null, null);
    DomainEntity trading =
        domainManager.createSubdomain(METALAKE, "Finance", "Trading", "comment", null);
    Assertions.assertEquals("Finance", trading.parent().get());
    Assertions.assertEquals(Domain.Status.DRAFT, trading.status());
    Assertions.assertEquals(trading, domainManager.getDomain(METALAKE, "Trading"));
    Assertions.assertArrayEquals(
        new String[] {"Trading"}, domainManager.listSubdomains(METALAKE, "Finance"));
    Assertions.assertArrayEquals(
        new DomainEntity[] {trading}, domainManager.listSubdomainInfos(METALAKE, "Finance"));
    Assertions.assertEquals(0, domainManager.listSubdomains(METALAKE, "Trading").length);

    // A sub-domain cannot have sub-domains.
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> domainManager.createSubdomain(METALAKE, "Trading", "Desk", null, null));
    Assertions.assertTrue(e.getMessage().contains("itself a sub-domain"), e.getMessage());
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.getDomain(METALAKE, "Desk"));

    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.createSubdomain(METALAKE, "missing", "Desk", null, null));
    // A domain cannot be its own parent.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.createSubdomain(METALAKE, "Finance", "Finance", null, null));
    Assertions.assertThrows(
        DomainAlreadyExistsException.class,
        () -> domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.listSubdomains(METALAKE, "missing"));
  }

  @Test
  public void testAlterDomain() {
    domainManager.createDomain(METALAKE, "Finance", "comment", ImmutableMap.of("k1", "v1"));
    domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null);
    domainManager.createDomain(METALAKE, "Markets", null, null);

    DomainEntity altered =
        domainManager.alterDomain(
            METALAKE,
            "Finance",
            DomainChange.rename("Finance2"),
            DomainChange.updateComment("new comment"),
            DomainChange.setProperty("k2", "v2"),
            DomainChange.removeProperty("k1"));
    Assertions.assertEquals("Finance2", altered.name());
    Assertions.assertEquals("new comment", altered.comment());
    Assertions.assertEquals(ImmutableMap.of("k2", "v2"), altered.properties());
    Assertions.assertEquals(Domain.Status.DRAFT, altered.status());
    Assertions.assertNotNull(altered.auditInfo().lastModifier());
    Assertions.assertEquals(altered, domainManager.getDomain(METALAKE, "Finance2"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.getDomain(METALAKE, "Finance"));
    // The sub-domain follows the renamed parent, and a sub-domain keeps its parent when altered.
    Assertions.assertEquals(
        "Finance2", domainManager.getDomain(METALAKE, "Trading").parent().get());
    Assertions.assertEquals(
        "Finance2",
        domainManager
            .alterDomain(METALAKE, "Trading", DomainChange.updateComment("c"))
            .parent()
            .get());

    Assertions.assertThrows(
        DomainAlreadyExistsException.class,
        () -> domainManager.alterDomain(METALAKE, "Finance2", DomainChange.rename("Markets")));
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.alterDomain(METALAKE, "missing", DomainChange.updateComment("c")));
  }

  @Test
  public void testDeleteDomain() {
    domainManager.createDomain(METALAKE, "Finance", null, null);
    domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null);

    Assertions.assertThrows(
        NonEmptyDomainException.class, () -> domainManager.deleteDomain(METALAKE, "Finance"));
    Assertions.assertNotNull(domainManager.getDomain(METALAKE, "Finance"));

    Assertions.assertTrue(domainManager.deleteDomain(METALAKE, "Trading"));
    Assertions.assertEquals(0, domainManager.listSubdomains(METALAKE, "Finance").length);
    Assertions.assertTrue(domainManager.deleteDomain(METALAKE, "Finance"));
    Assertions.assertFalse(domainManager.deleteDomain(METALAKE, "Finance"));
    Assertions.assertEquals(0, domainManager.listDomains(METALAKE).length);
  }

  @Test
  public void testPublishAndUnpublish() {
    domainManager.createDomain(METALAKE, "Finance", null, null);
    domainManager.createSubdomain(METALAKE, "Finance", "Trading", null, null);

    // A sub-domain needs a published parent.
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> domainManager.publishDomain(METALAKE, "Trading"));
    Assertions.assertEquals(
        Domain.Status.DRAFT, domainManager.getDomain(METALAKE, "Trading").status());

    DomainEntity finance = domainManager.publishDomain(METALAKE, "Finance");
    Assertions.assertEquals(Domain.Status.PUBLISHED, finance.status());
    Assertions.assertNotNull(finance.auditInfo().lastModifier());
    // Publishing a published domain changes nothing.
    Assertions.assertEquals(finance, domainManager.publishDomain(METALAKE, "Finance"));
    DomainEntity trading = domainManager.publishDomain(METALAKE, "Trading");
    Assertions.assertEquals(Domain.Status.PUBLISHED, trading.status());
    Assertions.assertEquals("Finance", trading.parent().get());

    // A domain with published sub-domains cannot be unpublished.
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> domainManager.unpublishDomain(METALAKE, "Finance"));
    Assertions.assertTrue(e.getMessage().contains("[Trading]"), e.getMessage());

    Assertions.assertEquals(
        Domain.Status.DRAFT, domainManager.unpublishDomain(METALAKE, "Trading").status());
    Assertions.assertEquals(
        Domain.Status.DRAFT, domainManager.unpublishDomain(METALAKE, "Finance").status());
    // Unpublishing a draft domain changes nothing.
    Assertions.assertEquals(
        Domain.Status.DRAFT, domainManager.unpublishDomain(METALAKE, "Finance").status());

    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.publishDomain(METALAKE, "missing"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.unpublishDomain(METALAKE, "missing"));
  }

  @Test
  public void testAttachMoveAndDetach() {
    domainManager.createDomain(METALAKE, "Finance", null, null);
    domainManager.createDomain(METALAKE, "Markets", null, null);
    domainManager.createDomain(METALAKE, "Trading", null, null);

    DomainEntity attached = domainManager.attachDomain(METALAKE, "Trading", "Finance");
    Assertions.assertEquals("Finance", attached.parent().get());
    Assertions.assertArrayEquals(
        new String[] {"Trading"}, domainManager.listSubdomains(METALAKE, "Finance"));
    // Attaching a domain to its current parent changes nothing.
    Assertions.assertEquals(attached, domainManager.attachDomain(METALAKE, "Trading", "Finance"));

    // A domain has one parent, so attaching a sub-domain moves it.
    DomainEntity moved = domainManager.attachDomain(METALAKE, "Trading", "Markets");
    Assertions.assertEquals("Markets", moved.parent().get());
    Assertions.assertEquals(0, domainManager.listSubdomains(METALAKE, "Finance").length);
    Assertions.assertArrayEquals(
        new String[] {"Trading"}, domainManager.listSubdomains(METALAKE, "Markets"));

    // A domain cannot be its own parent.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.attachDomain(METALAKE, "Finance", "Finance"));
    // The parent must be top-level.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.attachDomain(METALAKE, "Finance", "Trading"));
    // A domain with sub-domains cannot be attached.
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> domainManager.attachDomain(METALAKE, "Markets", "Finance"));
    Assertions.assertTrue(e.getMessage().contains("[Trading]"), e.getMessage());
    Assertions.assertFalse(domainManager.getDomain(METALAKE, "Markets").parent().isPresent());
    // A sub-domain cannot be moved under another sub-domain.
    domainManager.createSubdomain(METALAKE, "Finance", "Desk", null, null);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.attachDomain(METALAKE, "Trading", "Desk"));
    Assertions.assertEquals("Markets", domainManager.getDomain(METALAKE, "Trading").parent().get());

    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.attachDomain(METALAKE, "Trading", "missing"));
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> domainManager.attachDomain(METALAKE, "missing", "Finance"));

    DomainEntity detached = domainManager.detachDomain(METALAKE, "Trading");
    Assertions.assertFalse(detached.parent().isPresent());
    Assertions.assertEquals(0, domainManager.listSubdomains(METALAKE, "Markets").length);
    // Detaching a top-level domain changes nothing.
    Assertions.assertEquals(detached, domainManager.detachDomain(METALAKE, "Trading"));
    Assertions.assertThrows(
        NoSuchDomainException.class, () -> domainManager.detachDomain(METALAKE, "missing"));
  }

  @Test
  public void testAttachChecksStatus() {
    domainManager.createDomain(METALAKE, "Finance", null, null);
    domainManager.createDomain(METALAKE, "Trading", null, null);
    domainManager.publishDomain(METALAKE, "Trading");

    // A published domain needs a published parent.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.attachDomain(METALAKE, "Trading", "Finance"));
    Assertions.assertFalse(domainManager.getDomain(METALAKE, "Trading").parent().isPresent());

    domainManager.publishDomain(METALAKE, "Finance");
    Assertions.assertEquals(
        "Finance", domainManager.attachDomain(METALAKE, "Trading", "Finance").parent().get());

    // A draft domain can be attached under a draft or a published parent.
    domainManager.createDomain(METALAKE, "Draft", null, null);
    domainManager.createDomain(METALAKE, "Desk", null, null);
    Assertions.assertEquals(
        "Draft", domainManager.attachDomain(METALAKE, "Desk", "Draft").parent().get());
    Assertions.assertEquals(
        "Finance", domainManager.attachDomain(METALAKE, "Desk", "Finance").parent().get());

    // A published sub-domain cannot be moved under a draft parent.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domainManager.attachDomain(METALAKE, "Trading", "Draft"));
    Assertions.assertEquals("Finance", domainManager.getDomain(METALAKE, "Trading").parent().get());
    Assertions.assertEquals(0, domainManager.listSubdomains(METALAKE, "Draft").length);
  }

  @Test
  public void testChecksSkipUnchangedState() {
    DomainEntity draftParent = domain("Finance", null, Domain.Status.DRAFT);
    DomainEntity publishedChild = domain("Trading", "Finance", Domain.Status.PUBLISHED);
    DomainEntity grandParent = domain("Markets", "Other", Domain.Status.DRAFT);

    // Attaching a domain to its current parent is not checked.
    DomainManager.checkAttach(
        new LockedDomainState(publishedChild, draftParent, Collections.emptyList()));
    // A change of parent is.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DomainManager.checkAttach(
                new LockedDomainState(
                    domain("Trading", null, Domain.Status.PUBLISHED),
                    draftParent,
                    Collections.emptyList())));
    // So is a move from another parent.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DomainManager.checkAttach(
                new LockedDomainState(
                    domain("Trading", "Markets", Domain.Status.PUBLISHED),
                    draftParent,
                    Collections.emptyList())));
    // Two names that find the same row are the domain itself.
    DomainEntity finance = domain("Finance", null, Domain.Status.DRAFT);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DomainManager.checkAttach(
                new LockedDomainState(finance, finance, Collections.emptyList())));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DomainManager.checkNewDomain(
                new LockedDomainState(
                    domain("Desk", "Markets", Domain.Status.DRAFT),
                    grandParent,
                    Collections.emptyList())));

    // Publishing a published domain and unpublishing a draft one are not checked.
    DomainManager.checkPublish(
        new LockedDomainState(publishedChild, draftParent, Collections.emptyList()));
    List<DomainEntity> publishedSubDomains = Collections.singletonList(publishedChild);
    DomainManager.checkUnpublish(new LockedDomainState(draftParent, null, publishedSubDomains));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DomainManager.checkUnpublish(
                new LockedDomainState(
                    domain("Finance", null, Domain.Status.PUBLISHED), null, publishedSubDomains)));
  }

  @Test
  public void testCheckDomainMetadataObject() {
    domainManager.createDomain(METALAKE, "Finance", null, null);
    MetadataObjectUtil.checkMetadataObject(
        METALAKE, MetadataObjects.of(null, "Finance", MetadataObject.Type.DOMAIN));
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            MetadataObjectUtil.checkMetadataObject(
                METALAKE, MetadataObjects.of(null, "missing", MetadataObject.Type.DOMAIN)));
  }

  private static DomainEntity domain(String name, String parent, Domain.Status status) {
    return DomainEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain(METALAKE))
        .withProperties(Collections.emptyMap())
        .withStatus(status)
        .withParent(parent)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  private static Config mockConfig() {
    Config config = Mockito.mock(Config.class);

    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(
            String.format(
                "jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL;LOCK_TIMEOUT=30000", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_IDLE_CONNECTIONS)).thenReturn(10);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS)).thenReturn(3L);
    Mockito.when(config.get(ENTITY_CHANGE_LOG_POLL_BATCH_SIZE))
        .thenReturn(DEFAULT_ENTITY_CHANGE_LOG_POLL_BATCH_SIZE);
    Mockito.when(config.get(ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS))
        .thenReturn(DEFAULT_ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS);
    Mockito.when(config.get(ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES))
        .thenReturn(DEFAULT_ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES);
    Mockito.when(config.get(ENTITY_CHANGE_LOG_RETENTION_SECS)).thenReturn(24 * 60 * 60L);
    Mockito.when(config.get(ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)).thenReturn(60 * 60L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");
    Mockito.when(config.get(Configs.CACHE_LOCK_SEGMENTS)).thenReturn(16);

    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    return config;
  }
}
