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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.catalog.FilesetNormalizeDispatcher;
import org.apache.gravitino.catalog.FilesetOperationDispatcher;
import org.apache.gravitino.catalog.FunctionNormalizeDispatcher;
import org.apache.gravitino.catalog.FunctionOperationDispatcher;
import org.apache.gravitino.catalog.ModelNormalizeDispatcher;
import org.apache.gravitino.catalog.ModelOperationDispatcher;
import org.apache.gravitino.catalog.PartitionNormalizeDispatcher;
import org.apache.gravitino.catalog.PartitionOperationDispatcher;
import org.apache.gravitino.catalog.SchemaNormalizeDispatcher;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.catalog.SemanticModelNormalizeDispatcher;
import org.apache.gravitino.catalog.SemanticModelOperationDispatcher;
import org.apache.gravitino.catalog.TableNormalizeDispatcher;
import org.apache.gravitino.catalog.TableOperationDispatcher;
import org.apache.gravitino.catalog.TopicNormalizeDispatcher;
import org.apache.gravitino.catalog.TopicOperationDispatcher;
import org.apache.gravitino.catalog.ViewNormalizeDispatcher;
import org.apache.gravitino.catalog.ViewOperationDispatcher;
import org.apache.gravitino.hook.FilesetHookDispatcher;
import org.apache.gravitino.hook.FunctionHookDispatcher;
import org.apache.gravitino.hook.ModelHookDispatcher;
import org.apache.gravitino.hook.SchemaHookDispatcher;
import org.apache.gravitino.hook.SemanticModelHookDispatcher;
import org.apache.gravitino.hook.TableHookDispatcher;
import org.apache.gravitino.hook.TopicHookDispatcher;
import org.apache.gravitino.hook.ViewHookDispatcher;
import org.apache.gravitino.listener.FilesetEventDispatcher;
import org.apache.gravitino.listener.FunctionEventDispatcher;
import org.apache.gravitino.listener.ModelEventDispatcher;
import org.apache.gravitino.listener.PartitionEventDispatcher;
import org.apache.gravitino.listener.SchemaEventDispatcher;
import org.apache.gravitino.listener.StatisticEventDispatcher;
import org.apache.gravitino.listener.TableEventDispatcher;
import org.apache.gravitino.listener.TopicEventDispatcher;
import org.apache.gravitino.listener.ViewEventDispatcher;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.stats.StatisticManager;
import org.apache.gravitino.stats.storage.MemoryPartitionStatsStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestGravitinoEnvMetadataComponents {

  private Map<Field, Object> singletonState;

  @BeforeEach
  void isolateSingletonState() throws IllegalAccessException {
    GravitinoEnv singleton = GravitinoEnv.getInstance();
    singletonState = snapshotState(singleton);
    restoreState(singleton, snapshotState(new TestGravitinoEnv()));
  }

  @AfterEach
  void restoreSingletonState() throws IllegalAccessException {
    restoreState(GravitinoEnv.getInstance(), singletonState);
  }

  @Test
  void testMetadataProfileRejectsNonSingletonEnvironment() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> new TestGravitinoEnv().initializeMetadataComponents(metadataConfig(false)));

    assertEquals(
        "Metadata components must be initialized on GravitinoEnv.getInstance().",
        exception.getMessage());
  }

  @Test
  void testInternalPartitionAndStatisticDispatchersRequireInitialization() {
    GravitinoEnv env = GravitinoEnv.getInstance();

    assertThrows(IllegalArgumentException.class, env::internalPartitionDispatcher);
    assertThrows(IllegalArgumentException.class, env::internalStatisticDispatcher);
  }

  @Test
  void testMetadataProfileProvidesCompleteMetadataAccessWithoutServerServices() throws Exception {
    Config config = metadataConfig(false);
    EntityStore entityStore = relationStore();
    GravitinoEnv env = GravitinoEnv.getInstance();

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
      assertDispatcherChain(
          env.semanticModelDispatcher(),
          SemanticModelNormalizeDispatcher.class,
          SemanticModelOperationDispatcher.class);
      assertNotNull(env.credentialOperationDispatcher());
      assertNotNull(env.secretPropertyOperationDispatcher());
      assertNotNull(env.internalTagDispatcher());
      assertNotNull(env.internalPolicyDispatcher());
      assertInstanceOf(StatisticManager.class, env.internalStatisticDispatcher());
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
      assertNull(env.statisticDispatcher());
      assertNull(env.internalAccessControlDispatcher());
      assertNull(env.internalOwnerDispatcher());
      assertNull(env.bulkManager());
      assertNull(env.futureGrantManager());
      assertThrows(IllegalArgumentException.class, env::eventBus);
      assertThrows(IllegalArgumentException.class, env::jobOperationDispatcher);
      assertThrows(IllegalArgumentException.class, env::internalJobOperationDispatcher);

      verify(entityStore).initialize(config);
      clearInvocations(entityStore);
      assertEquals(0, env.internalMetalakeDispatcher().listMetalakes().length);
      verify(entityStore).list(Namespace.empty(), BaseMetalake.class, EntityType.METALAKE);

      assertDoesNotThrow(env::start);
    } finally {
      env.shutdown();
    }

    verify(entityStore).close();
  }

  @Test
  void testMetadataProfileProvidesInternalAuthorizationWhenEnabled() throws Exception {
    Config config = metadataConfig(true);
    EntityStore entityStore = relationStore();
    GravitinoEnv env = GravitinoEnv.getInstance();

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
    }

    verify(entityStore).close();
  }

  @Test
  void testFullProfilePreservesDispatcherChains() throws Exception {
    Config config = metadataConfig(false);
    EntityStore entityStore = relationStore();
    GravitinoEnv env = GravitinoEnv.getInstance();

    try (MockedStatic<EntityStoreFactory> entityStoreFactory =
        mockStatic(EntityStoreFactory.class)) {
      entityStoreFactory
          .when(() -> EntityStoreFactory.createEntityStore(config))
          .thenReturn(entityStore);

      env.initializeFullComponents(config);

      assertDispatcherChain(
          env.filesetDispatcher(),
          FilesetEventDispatcher.class,
          FilesetNormalizeDispatcher.class,
          FilesetHookDispatcher.class,
          FilesetOperationDispatcher.class);
      assertDispatcherChain(
          env.schemaDispatcher(),
          SchemaEventDispatcher.class,
          SchemaNormalizeDispatcher.class,
          SchemaHookDispatcher.class,
          SchemaOperationDispatcher.class);
      assertDispatcherChain(
          env.tableDispatcher(),
          TableEventDispatcher.class,
          TableNormalizeDispatcher.class,
          TableHookDispatcher.class,
          TableOperationDispatcher.class);
      assertDispatcherChain(
          env.topicDispatcher(),
          TopicEventDispatcher.class,
          TopicNormalizeDispatcher.class,
          TopicHookDispatcher.class,
          TopicOperationDispatcher.class);
      assertDispatcherChain(
          env.modelDispatcher(),
          ModelEventDispatcher.class,
          ModelNormalizeDispatcher.class,
          ModelHookDispatcher.class,
          ModelOperationDispatcher.class);
      assertDispatcherChain(
          env.functionDispatcher(),
          FunctionEventDispatcher.class,
          FunctionNormalizeDispatcher.class,
          FunctionHookDispatcher.class,
          FunctionOperationDispatcher.class);
      assertDispatcherChain(
          env.semanticModelDispatcher(),
          SemanticModelNormalizeDispatcher.class,
          SemanticModelHookDispatcher.class,
          SemanticModelOperationDispatcher.class);
      assertDispatcherChain(
          env.viewDispatcher(),
          ViewEventDispatcher.class,
          ViewNormalizeDispatcher.class,
          ViewHookDispatcher.class,
          ViewOperationDispatcher.class);
      assertDispatcherChain(
          env.partitionDispatcher(),
          PartitionEventDispatcher.class,
          PartitionNormalizeDispatcher.class,
          PartitionOperationDispatcher.class);
      assertSame(
          env.internalPartitionDispatcher(),
          FieldUtils.readField(env.partitionDispatcher(), "dispatcher", true));
      assertDispatcherChain(
          env.statisticDispatcher(), StatisticEventDispatcher.class, StatisticManager.class);
      assertSame(
          env.internalStatisticDispatcher(),
          FieldUtils.readField(env.statisticDispatcher(), "dispatcher", true));
    } finally {
      env.shutdown();
    }

    verify(entityStore).close();
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

  private static Map<Field, Object> snapshotState(GravitinoEnv env) throws IllegalAccessException {
    Map<Field, Object> state = new LinkedHashMap<>();
    for (Field field : FieldUtils.getAllFieldsList(GravitinoEnv.class)) {
      if (!Modifier.isStatic(field.getModifiers())) {
        state.put(field, FieldUtils.readField(field, env, true));
      }
    }
    return state;
  }

  private static void restoreState(GravitinoEnv env, Map<Field, Object> state)
      throws IllegalAccessException {
    for (Map.Entry<Field, Object> entry : state.entrySet()) {
      FieldUtils.writeField(entry.getKey(), env, entry.getValue(), true);
    }
  }

  private static void assertDispatcherChain(Object dispatcher, Class<?>... dispatcherClasses)
      throws IllegalAccessException {
    Object currentDispatcher = dispatcher;
    for (int index = 0; index < dispatcherClasses.length; index++) {
      assertInstanceOf(dispatcherClasses[index], currentDispatcher);
      if (index < dispatcherClasses.length - 1) {
        currentDispatcher = FieldUtils.readField(currentDispatcher, "dispatcher", true);
      }
    }
  }

  private static final class TestGravitinoEnv extends GravitinoEnv {}
}
