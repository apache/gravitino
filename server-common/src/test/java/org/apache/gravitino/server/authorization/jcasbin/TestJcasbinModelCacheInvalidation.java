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
package org.apache.gravitino.server.authorization.jcasbin;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.catalog.ModelNormalizeDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.hook.ModelHookDispatcher;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Regression tests for model name and descendant cache invalidation. */
public class TestJcasbinModelCacheInvalidation {

  private static final String METALAKE = "ml1";
  private static final MetadataObject MODEL =
      MetadataObjects.parse("cat1.sch1.model1", MetadataObject.Type.MODEL);
  private static final MetadataObject VERSION =
      MetadataObjects.parse("cat1.sch1.model1.0", MetadataObject.Type.MODEL_VERSION);
  private static final MetadataObject OTHER_VERSION =
      MetadataObjects.parse("cat1.sch1.model10.0", MetadataObject.Type.MODEL_VERSION);

  private static final Capability CASE_INSENSITIVE_CAPABILITY =
      new Capability() {
        @Override
        public CapabilityResult caseSensitiveOnName(Scope scope) {
          return scope == Scope.MODEL || scope == Scope.SCHEMA
              ? CapabilityResult.unsupported("Names are case insensitive")
              : CapabilityResult.SUPPORTED;
        }
      };

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testModelMutationInvalidatesModelAndVersions(boolean rename, boolean caseInsensitive)
      throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    CatalogManager catalogManager = mock(CatalogManager.class);
    when(env.catalogManager()).thenReturn(catalogManager);
    ModelDispatcher delegate = mock(ModelDispatcher.class);
    NameIdentifier normalizedIdent = NameIdentifier.of(METALAKE, "cat1", "sch1", "model1");
    when(delegate.deleteModel(normalizedIdent)).thenReturn(true);
    Model renamedModel = mock(Model.class);
    when(delegate.alterModel(eq(normalizedIdent), any(ModelChange[].class)))
        .thenReturn(renamedModel);
    Capability capability = caseInsensitive ? CASE_INSENSITIVE_CAPABILITY : Capability.DEFAULT;

    try (CaffeineGravitinoCache<String, Long> metadataIdCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        CaffeineGravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class);
        MockedStatic<CapabilityHelpers> capabilities =
            mockStatic(CapabilityHelpers.class, CALLS_REAL_METHODS);
        MockedStatic<MetadataIdConverter> converter = mockStatic(MetadataIdConverter.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      capabilities
          .when(() -> CapabilityHelpers.getCapability(any(), eq(catalogManager)))
          .thenReturn(capability);
      converter
          .when(() -> MetadataIdConverter.getID(MODEL, METALAKE))
          .thenReturn(Optional.of(10L), Optional.of(30L));
      converter
          .when(() -> MetadataIdConverter.getID(VERSION, METALAKE))
          .thenReturn(Optional.of(20L), Optional.of(40L));

      // Use the real JCasbin hook and shared caches without starting its background poller.
      JcasbinAuthorizer authorizer = mock(JcasbinAuthorizer.class, CALLS_REAL_METHODS);
      FieldUtils.writeField(authorizer, "metadataIdCache", metadataIdCache, true);
      when(env.gravitinoAuthorizer()).thenReturn(authorizer);
      JcasbinAuthorizationLookups lookups =
          new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
      MetadataObject requestedModel =
          caseInsensitive
              ? MetadataObjects.parse("cat1.SCH1.MODEL1", MetadataObject.Type.MODEL)
              : MODEL;
      MetadataObject requestedVersion =
          caseInsensitive
              ? MetadataObjects.parse("cat1.SCH1.MODEL1.0", MetadataObject.Type.MODEL_VERSION)
              : VERSION;
      if (caseInsensitive) {
        // The DB converter accepts either spelling even before cache keys are normalized.
        converter
            .when(() -> MetadataIdConverter.getID(requestedModel, METALAKE))
            .thenReturn(Optional.of(10L), Optional.of(30L));
        converter
            .when(() -> MetadataIdConverter.getID(requestedVersion, METALAKE))
            .thenReturn(Optional.of(20L), Optional.of(40L));
      }
      Assertions.assertEquals(
          Optional.of(10L),
          lookups.resolveMetadataId(requestedModel, METALAKE, new AuthorizationRequestContext()));
      Assertions.assertEquals(
          Optional.of(20L),
          lookups.resolveMetadataId(requestedVersion, METALAKE, new AuthorizationRequestContext()));
      // A different spelling of the same model must share the canonical cache entry.
      Assertions.assertEquals(
          Optional.of(10L),
          lookups.resolveMetadataId(MODEL, METALAKE, new AuthorizationRequestContext()));
      String otherKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, OTHER_VERSION);
      metadataIdCache.put(otherKey, 50L);

      ModelDispatcher dispatcher =
          new ModelNormalizeDispatcher(new ModelHookDispatcher(delegate), catalogManager);
      NameIdentifier requestedIdent =
          caseInsensitive ? NameIdentifier.of(METALAKE, "cat1", "SCH1", "MODEL1") : normalizedIdent;
      if (rename) {
        Assertions.assertSame(
            renamedModel, dispatcher.alterModel(requestedIdent, ModelChange.rename("renamed")));
      } else {
        Assertions.assertTrue(dispatcher.deleteModel(requestedIdent));
      }
      Assertions.assertFalse(
          metadataIdCache
              .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, MODEL))
              .isPresent());
      Assertions.assertFalse(
          metadataIdCache
              .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, VERSION))
              .isPresent());
      Assertions.assertFalse(
          metadataIdCache
              .getIfPresent(
                  JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, requestedModel))
              .isPresent());
      Assertions.assertFalse(
          metadataIdCache
              .getIfPresent(
                  JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, requestedVersion))
              .isPresent());
      Assertions.assertEquals(Optional.of(50L), metadataIdCache.getIfPresent(otherKey));

      // Simulate recreation under the old name, with fresh model and version IDs.
      Assertions.assertEquals(
          Optional.of(30L),
          lookups.resolveMetadataId(requestedModel, METALAKE, new AuthorizationRequestContext()));
      Assertions.assertEquals(
          Optional.of(40L),
          lookups.resolveMetadataId(requestedVersion, METALAKE, new AuthorizationRequestContext()));
      converter.verify(() -> MetadataIdConverter.getID(MODEL, METALAKE), times(2));
      converter.verify(() -> MetadataIdConverter.getID(VERSION, METALAKE), times(2));
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = OperateType.class,
      names = {"ALTER", "DROP"})
  void testModelChangeLogInvalidatesVersions(OperateType operation) {
    try (CaffeineGravitinoCache<String, Long> metadataIdCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        CaffeineGravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        JcasbinChangeListener listener =
            new JcasbinChangeListener(metadataIdCache, ownerRelCache, 1)) {
      String modelKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, MODEL);
      String versionKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, VERSION);
      String otherKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, OTHER_VERSION);
      metadataIdCache.put(modelKey, 10L);
      metadataIdCache.put(versionKey, 20L);
      metadataIdCache.put(otherKey, 50L);
      EntityChangeRecord change =
          new EntityChangeRecord(
              1L,
              METALAKE,
              "MODEL",
              EntityChangeLogNameIdentifierCodec.encode(
                  NameIdentifier.of(METALAKE, "cat1", "sch1", "model1")),
              operation,
              1L);
      listener.onEntityChange(List.of(change));
      Assertions.assertFalse(metadataIdCache.getIfPresent(modelKey).isPresent());
      Assertions.assertFalse(metadataIdCache.getIfPresent(versionKey).isPresent());
      Assertions.assertEquals(Optional.of(50L), metadataIdCache.getIfPresent(otherKey));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testOwnerChangeInvalidatesCanonicalModelCache(boolean normalizationFails) throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    CatalogManager catalogManager = mock(CatalogManager.class);
    when(env.catalogManager()).thenReturn(catalogManager);
    try (CaffeineGravitinoCache<String, Long> metadataIdCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        CaffeineGravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class);
        MockedStatic<CapabilityHelpers> capabilities =
            mockStatic(CapabilityHelpers.class, CALLS_REAL_METHODS);
        MockedStatic<MetadataIdConverter> converter = mockStatic(MetadataIdConverter.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      if (normalizationFails) {
        capabilities
            .when(() -> CapabilityHelpers.getCapability(any(), eq(catalogManager)))
            .thenThrow(new RuntimeException("Catalog unavailable"));
      } else {
        capabilities
            .when(() -> CapabilityHelpers.getCapability(any(), eq(catalogManager)))
            .thenReturn(CASE_INSENSITIVE_CAPABILITY);
      }
      converter.when(() -> MetadataIdConverter.getID(MODEL, METALAKE)).thenReturn(Optional.of(10L));
      metadataIdCache.put(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, MODEL), 10L);
      String otherKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, OTHER_VERSION);
      metadataIdCache.put(otherKey, 50L);
      ownerRelCache.put(10L, Optional.of(new OwnerInfo(100L, "USER")));
      JcasbinAuthorizer authorizer = mock(JcasbinAuthorizer.class, CALLS_REAL_METHODS);
      FieldUtils.writeField(authorizer, "metadataIdCache", metadataIdCache, true);
      FieldUtils.writeField(authorizer, "ownerRelCache", ownerRelCache, true);
      Assertions.assertDoesNotThrow(
          () ->
              authorizer.handleMetadataOwnerChange(
                  METALAKE,
                  100L,
                  NameIdentifier.of(METALAKE, "cat1", "SCH1", "MODEL1"),
                  Entity.EntityType.MODEL));
      Assertions.assertFalse(
          metadataIdCache
              .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, MODEL))
              .isPresent());
      Assertions.assertEquals(
          normalizationFails ? Optional.empty() : Optional.of(50L),
          metadataIdCache.getIfPresent(otherKey));
      Assertions.assertEquals(0L, ownerRelCache.size());
    }
  }

  @Test
  void testMissingCatalogDoesNotCacheModelId() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    CatalogManager catalogManager = mock(CatalogManager.class);
    when(env.catalogManager()).thenReturn(catalogManager);
    try (CaffeineGravitinoCache<String, Long> metadataIdCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        CaffeineGravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class);
        MockedStatic<CapabilityHelpers> capabilities =
            mockStatic(CapabilityHelpers.class, CALLS_REAL_METHODS)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      capabilities
          .when(() -> CapabilityHelpers.getCapability(any(), eq(catalogManager)))
          .thenThrow(new NoSuchCatalogException("Catalog does not exist"));
      JcasbinAuthorizationLookups lookups =
          new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
      Assertions.assertEquals(
          Optional.empty(),
          lookups.resolveMetadataId(MODEL, METALAKE, new AuthorizationRequestContext()));
      Assertions.assertEquals(
          Optional.empty(),
          lookups.resolveMetadataId(VERSION, METALAKE, new AuthorizationRequestContext()));
      Assertions.assertEquals(0L, metadataIdCache.size());
    }
  }
}
