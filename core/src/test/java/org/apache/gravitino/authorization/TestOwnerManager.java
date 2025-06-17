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
package org.apache.gravitino.authorization;

import static org.apache.gravitino.Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
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
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestOwnerManager {
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final String METALAKE = "metalake_for_owner_test";
  private static final String USER = "user";
  private static final String GROUP = "group";
  private static final Config config = Mockito.mock(Config.class);
  private static EntityStore entityStore;

  private static IdGenerator idGenerator;
  private static OwnerManager ownerManager;
  private static CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
  private static AuthorizationPlugin authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);

  @BeforeAll
  public static void setUp() throws IOException, IllegalAccessException {
    idGenerator = new RandomIdGenerator();

    File dbDir = new File(DB_DIR);
    dbDir.mkdirs();

    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    Mockito.when(config.get(CATALOG_CACHE_EVICTION_INTERVAL_MS)).thenReturn(1000L);
    // Fix the cache config for testing
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);

    entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE)
            .withVersion(SchemaVersion.V_0_1)
            .withComment("Test metalake")
            .withAuditInfo(audit)
            .build();
    entityStore.put(metalake, false /* overwritten */);

    UserEntity userEntity =
        UserEntity.builder()
            .withId(idGenerator.nextId())
            .withName(USER)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withNamespace(AuthorizationUtils.ofUserNamespace(METALAKE))
            .withAuditInfo(audit)
            .build();
    entityStore.put(userEntity, false /* overwritten*/);

    GroupEntity groupEntity =
        GroupEntity.builder()
            .withId(idGenerator.nextId())
            .withName(GROUP)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withNamespace(AuthorizationUtils.ofUserNamespace(METALAKE))
            .withAuditInfo(audit)
            .build();
    entityStore.put(groupEntity, false /* overwritten*/);

    ownerManager = new OwnerManager(entityStore);
    BaseCatalog catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    Mockito.when(catalogManager.listCatalogs(Mockito.any()))
        .thenReturn(new NameIdentifier[] {NameIdentifier.of("metalake", "catalog")});
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }

    FileUtils.deleteDirectory(new File(JDBC_STORE_PATH));
  }

  @Test
  public void testOwner() {
    // Test no owner
    MetadataObject metalakeObject =
        MetadataObjects.of(Lists.newArrayList(METALAKE), MetadataObject.Type.METALAKE);
    Assertions.assertFalse(ownerManager.getOwner(METALAKE, metalakeObject).isPresent());
    Mockito.verify(authorizationPlugin, Mockito.never())
        .onOwnerSet(Mockito.any(), Mockito.any(), Mockito.any());

    // Test not-existed metadata object
    MetadataObject notExistObject =
        MetadataObjects.of(Lists.newArrayList("not-exist"), MetadataObject.Type.CATALOG);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class, () -> ownerManager.getOwner(METALAKE, notExistObject));

    // Test to set the user as the owner
    ownerManager.setOwner(METALAKE, metalakeObject, USER, Owner.Type.USER);
    Mockito.verify(authorizationPlugin).onOwnerSet(Mockito.any(), Mockito.any(), Mockito.any());

    Owner owner = ownerManager.getOwner(METALAKE, metalakeObject).get();
    Assertions.assertEquals(USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Test to set the group as the owner
    Mockito.reset(authorizationPlugin);
    ownerManager.setOwner(METALAKE, metalakeObject, GROUP, Owner.Type.GROUP);
    Mockito.verify(authorizationPlugin).onOwnerSet(Mockito.any(), Mockito.any(), Mockito.any());

    // Test not-existed metadata object
    Assertions.assertThrows(
        NotFoundException.class,
        () -> ownerManager.setOwner(METALAKE, notExistObject, GROUP, Owner.Type.GROUP));

    owner = ownerManager.getOwner(METALAKE, metalakeObject).get();
    Assertions.assertEquals(GROUP, owner.name());
    Assertions.assertEquals(Owner.Type.GROUP, owner.type());
  }
}
