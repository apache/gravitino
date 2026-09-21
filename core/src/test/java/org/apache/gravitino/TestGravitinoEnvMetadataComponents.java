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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.stats.StatisticManager;
import org.apache.gravitino.stats.storage.MemoryPartitionStatsStorageFactory;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestGravitinoEnvMetadataComponents {

  @Test
  void testMetadataProfileProvidesCompleteMetadataAccessWithoutServerServices() throws Exception {
    Config config = metadataConfig(false);
    EntityStore entityStore = relationStore();
    TestGravitinoEnv env = new TestGravitinoEnv();
    Object originalLockManager = installSingletonLock(config);

    try (MockedStatic<EntityStoreFactory> entityStoreFactory =
        mockStatic(EntityStoreFactory.class)) {
      entityStoreFactory
          .when(() -> EntityStoreFactory.createEntityStore(config))
          .thenReturn(entityStore);

      env.initializeMetadataComponents(config);

      assertSame(entityStore, env.entityStore());
      assertNotNull(env.catalogManager());
      assertNotNull(env.internalMetalakeDispatcher());
      assertNotNull(env.internalCatalogDispatcher());
      assertNotNull(env.internalFilesetDispatcher());
      assertNotNull(env.internalSchemaDispatcher());
      assertNotNull(env.internalTableDispatcher());
      assertNotNull(env.internalPartitionDispatcher());
      assertNotNull(env.internalTopicDispatcher());
      assertNotNull(env.internalModelDispatcher());
      assertNotNull(env.internalFunctionDispatcher());
      assertNotNull(env.internalViewDispatcher());
      assertNotNull(env.semanticModelDispatcher());
      assertNotNull(env.credentialOperationDispatcher());
      assertNotNull(env.secretPropertyOperationDispatcher());
      assertNotNull(env.internalTagDispatcher());
      assertNotNull(env.internalPolicyDispatcher());
      assertInstanceOf(StatisticManager.class, env.statisticDispatcher());
      assertNotNull(env.lockManager());
      assertNotNull(env.metricsSystem());
      assertNotNull(env.secretManager());

      assertNull(env.auxServiceManager());
      assertNull(env.eventListenerManager());
      assertNull(env.metalakeDispatcher());
      assertNull(env.catalogDispatcher());
      assertNull(env.filesetDispatcher());
      assertNull(env.schemaDispatcher());
      assertNull(env.tableDispatcher());
      assertNull(env.partitionDispatcher());
      assertNull(env.topicDispatcher());
      assertNull(env.modelDispatcher());
      assertNull(env.functionDispatcher());
      assertNull(env.viewDispatcher());
      assertNull(env.tagDispatcher());
      assertNull(env.policyDispatcher());
      assertNull(env.internalAccessControlDispatcher());
      assertNull(env.internalOwnerDispatcher());
      assertNull(env.bulkManager());
      assertNull(env.futureGrantManager());
      assertThrows(IllegalArgumentException.class, env::eventBus);
      assertThrows(IllegalArgumentException.class, env::jobOperationDispatcher);
      assertThrows(IllegalArgumentException.class, env::internalJobOperationDispatcher);

      assertDoesNotThrow(env::start);
      verify(entityStore).initialize(config);
    } finally {
      env.shutdown();
      FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", originalLockManager, true);
    }

    verify(entityStore).close();
  }

  @Test
  void testMetadataProfileProvidesInternalAuthorizationWhenEnabled() throws Exception {
    Config config = metadataConfig(true);
    EntityStore entityStore = relationStore();
    TestGravitinoEnv env = new TestGravitinoEnv();
    Object originalLockManager = installSingletonLock(config);

    try (MockedStatic<EntityStoreFactory> entityStoreFactory =
        mockStatic(EntityStoreFactory.class)) {
      entityStoreFactory
          .when(() -> EntityStoreFactory.createEntityStore(config))
          .thenReturn(entityStore);

      env.initializeMetadataComponents(config);

      assertNotNull(env.internalAccessControlDispatcher());
      assertNotNull(env.internalOwnerDispatcher());
      assertNotNull(env.bulkManager());
      assertNotNull(env.futureGrantManager());
      assertNull(env.accessControlDispatcher());
      assertNull(env.ownerDispatcher());
    } finally {
      env.shutdown();
      FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", originalLockManager, true);
    }
  }

  private static Config metadataConfig(boolean enableAuthorization) {
    Config config = new Config(false) {};
    config.set(Configs.ENABLE_AUTHORIZATION, enableAuthorization);
    config.set(Configs.SERVICE_ADMINS, Collections.singletonList("admin"));
    config.set(
        Configs.PARTITION_STATS_STORAGE_FACTORY_CLASS,
        MemoryPartitionStatsStorageFactory.class.getCanonicalName());
    return config;
  }

  private static EntityStore relationStore() {
    return mock(
        EntityStore.class, withSettings().extraInterfaces(SupportsRelationOperations.class));
  }

  private static Object installSingletonLock(Config config) throws IllegalAccessException {
    Object originalLockManager =
        FieldUtils.readField(GravitinoEnv.getInstance(), "lockManager", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    return originalLockManager;
  }

  private static final class TestGravitinoEnv extends GravitinoEnv {}
}
