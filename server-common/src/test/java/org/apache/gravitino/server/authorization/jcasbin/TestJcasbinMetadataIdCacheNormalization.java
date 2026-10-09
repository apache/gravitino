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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.hook.SchemaHookDispatcher;
import org.apache.gravitino.hook.TableHookDispatcher;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.utils.EntityClassMapper;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

/** Verifies capability-normalized authorization cache keys through local and peer invalidation. */
public class TestJcasbinMetadataIdCacheNormalization {
  private static final String METALAKE = "metalake";
  private static final Capability CASE_INSENSITIVE =
      new Capability() {
        @Override
        public CapabilityResult caseSensitiveOnName(Scope scope) {
          return CapabilityResult.unsupported("case insensitive");
        }
      };

  private final Map<NameIdentifier, Entity> entities = new HashMap<>();
  private CaffeineGravitinoCache<String, Long> metadataCache;
  private CaffeineGravitinoCache<Long, Optional<OwnerInfo>> ownerCache;
  private CatalogManager catalogs;
  private BaseCatalog<?> catalog;
  private EntityStore store;
  private GravitinoEnv env;
  private JcasbinAuthorizationLookups lookups;
  private GravitinoAuthorizer authorizer;
  private MockedStatic<GravitinoEnv> envMock;

  @BeforeEach
  void setup() throws Exception {
    metadataCache = new CaffeineGravitinoCache<>(60_000L, 100L);
    ownerCache = new CaffeineGravitinoCache<>(60_000L, 100L);
    lookups = new JcasbinAuthorizationLookups(metadataCache, ownerCache);
    JcasbinAuthorizer localAuthorizer = new JcasbinAuthorizer();
    // The name-ID hook only needs the same cache owned by the initialized authorizer.
    FieldUtils.writeDeclaredField(localAuthorizer, "metadataIdCache", metadataCache, true);
    authorizer = localAuthorizer;
    catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(CASE_INSENSITIVE);
    when(catalog.provider()).thenReturn("test");
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    catalogs = mock(CatalogManager.class);
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    store = mock(EntityStore.class);
    when(store.get(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              NameIdentifier ident = invocation.getArgument(0);
              Entity entity = entities.get(ident);
              if (entity == null) {
                throw new NoSuchEntityException("Missing entity: %s", ident);
              }
              return entity;
            });
    env = mock(GravitinoEnv.class);
    when(env.entityStore()).thenReturn(store);
    when(env.catalogManager()).thenReturn(catalogs);
    when(env.gravitinoAuthorizer()).thenReturn(authorizer);
    when(env.internalAccessControlDispatcher()).thenReturn(mock(AccessControlDispatcher.class));
    CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
    when(catalogDispatcher.loadCatalog(any())).thenReturn(catalog);
    when(env.internalCatalogDispatcher()).thenReturn(catalogDispatcher);
    SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
    when(schemaDispatcher.loadSchema(any())).thenReturn(mock(Schema.class));
    when(env.internalSchemaDispatcher()).thenReturn(schemaDispatcher);
    envMock = mockStatic(GravitinoEnv.class);
    envMock.when(GravitinoEnv::getInstance).thenReturn(env);
  }

  @AfterEach
  void cleanup() {
    if (envMock != null) {
      envMock.close();
    }
    metadataCache.close();
    ownerCache.close();
  }

  @ParameterizedTest
  @MethodSource("catalogScopedTypes")
  void testAllMappedTypesNormalizeNamesIncludingColumnParents(MetadataObject.Type type) {
    assertEquals(
        object(type, false),
        MetadataIdConverter.normalizeMetadataObject(object(type, true), METALAKE));
  }

  @ParameterizedTest
  @MethodSource("idLookupTypes")
  void testLocalNotificationEvictsAliasesAfterNameReuse(MetadataObject.Type type)
      throws IOException {
    MetadataObject alias = object(type, true);
    MetadataObject normalized = object(type, false);
    put(normalized, 100L);
    assertAliasesShareBothCacheTiers(alias, normalized);
    AuthorizationUtils.notifyEntityNameIdMappingChange(ident(normalized), entityType(type));
    assertEquals(0L, metadataCache.size());
    entities.remove(ident(normalized));
    assertEquals(Optional.empty(), resolve(alias));
    put(normalized, 200L);
    assertEquals(Optional.of(200L), resolve(alias));
    assertEquals(Optional.of(200L), resolve(normalized));
  }

  @ParameterizedTest
  @MethodSource("idLookupTypes")
  void testPeerReplayEvictsAliasesAfterRenameAndNameReuse(MetadataObject.Type type)
      throws IOException {
    MetadataObject alias = object(type, true);
    MetadataObject normalized = object(type, false);
    put(normalized, 100L);
    assertAliasesShareBothCacheTiers(alias, normalized);
    try (JcasbinChangeListener listener =
        new JcasbinChangeListener(metadataCache, ownerCache, 3600L)) {
      listener.onEntityChange(List.of(change(normalized, OperateType.ALTER)));
    }
    assertEquals(0L, metadataCache.size());
    entities.remove(ident(normalized));
    assertEquals(Optional.empty(), resolve(alias));
    put(normalized, 300L);
    assertEquals(Optional.of(300L), resolve(alias));
    assertEquals(Optional.of(300L), resolve(normalized));
  }

  @ParameterizedTest
  @MethodSource("idLookupTypes")
  void testCaseSensitiveCatalogKeepsNamesDistinct(MetadataObject.Type type) {
    when(catalog.capability()).thenReturn(Capability.DEFAULT);
    MetadataObject upper = object(type, true);
    MetadataObject lower = object(type, false);
    assertSame(upper, MetadataIdConverter.normalizeMetadataObject(upper, METALAKE));
    put(upper, 100L);
    put(lower, 200L);
    assertEquals(Optional.of(100L), resolve(upper));
    assertEquals(Optional.of(200L), resolve(lower));
    assertEquals(2L, metadataCache.size());
    AuthorizationUtils.notifyEntityNameIdMappingChange(ident(upper), entityType(type));
    put(upper, 300L);
    assertEquals(Optional.of(300L), resolve(upper));
    assertEquals(Optional.of(200L), resolve(lower));
  }

  @Test
  void testTableDropHookEvictsTableAndColumnAliases() throws Exception {
    MetadataObject table = object(MetadataObject.Type.TABLE, false);
    MetadataObject column = object(MetadataObject.Type.COLUMN, false);
    put(table, 100L);
    // EntityStore does not independently load COLUMN IDs. Seed a descendant key to verify the
    // hook's prefix eviction, without inventing a ColumnEntity that implements HasIdentifier.
    metadataCache.put(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, column), 200L);
    assertEquals(Optional.of(100L), resolve(object(MetadataObject.Type.TABLE, true)));
    TableDispatcher dispatcher = mock(TableDispatcher.class);
    when(dispatcher.dropTable(ident(table))).thenReturn(true);
    assertTrue(new TableHookDispatcher(dispatcher, () -> null).dropTable(ident(table)));
    assertEquals(0L, metadataCache.size());
    put(table, 300L);
    assertEquals(Optional.of(300L), resolve(object(MetadataObject.Type.TABLE, true)));
    assertFalse(
        metadataCache
            .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, column))
            .isPresent());
  }

  @Test
  void testTableRenameHookEvictsOldAliasAndAllowsNameReuse() throws Exception {
    MetadataObject old = object(MetadataObject.Type.TABLE, false);
    MetadataObject alias = object(MetadataObject.Type.TABLE, true);
    MetadataObject renamed = MetadataObjects.parse("cat.schema.renamed", MetadataObject.Type.TABLE);
    put(old, 100L);
    assertEquals(Optional.of(100L), resolve(alias));
    TableDispatcher dispatcher = mock(TableDispatcher.class);
    TableChange rename = TableChange.rename("renamed");
    when(dispatcher.alterTable(ident(old), rename)).thenReturn(mock(Table.class));
    new TableHookDispatcher(dispatcher, () -> null).alterTable(ident(old), rename);
    entities.remove(ident(old));
    put(renamed, 100L);
    assertEquals(Optional.empty(), resolve(alias));
    assertEquals(Optional.of(100L), resolve(renamed));
    put(old, 200L);
    assertEquals(Optional.of(200L), resolve(alias));
  }

  @Test
  void testSchemaDropHookAndPeerReplayEvictDescendantAliases() throws Exception {
    MetadataObject schema = object(MetadataObject.Type.SCHEMA, false);
    MetadataObject table = object(MetadataObject.Type.TABLE, false);
    MetadataObject column = object(MetadataObject.Type.COLUMN, false);
    put(schema, 100L);
    put(table, 200L);
    metadataCache.put(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, column), 300L);
    resolve(object(MetadataObject.Type.SCHEMA, true));
    resolve(object(MetadataObject.Type.TABLE, true));
    SchemaDispatcher dispatcher = mock(SchemaDispatcher.class);
    when(dispatcher.dropSchema(ident(schema), true)).thenReturn(true);
    assertTrue(new SchemaHookDispatcher(dispatcher).dropSchema(ident(schema), true));
    assertEquals(0L, metadataCache.size());
    assertEquals(Optional.of(200L), resolve(object(MetadataObject.Type.TABLE, true)));
    metadataCache.put(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, column), 300L);
    try (JcasbinChangeListener listener =
        new JcasbinChangeListener(metadataCache, ownerCache, 3600L)) {
      listener.onEntityChange(List.of(change(schema, OperateType.DROP)));
    }
    assertEquals(0L, metadataCache.size());
    put(table, 400L);
    assertEquals(Optional.of(400L), resolve(object(MetadataObject.Type.TABLE, true)));
    assertFalse(
        metadataCache
            .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, column))
            .isPresent());
  }

  @Test
  void testColumnNormalizationHonorsEachScopeAndPreservesCatalogName() {
    when(catalog.capability())
        .thenReturn(
            new Capability() {
              @Override
              public CapabilityResult caseSensitiveOnName(Scope scope) {
                return scope == Scope.TABLE
                    ? CapabilityResult.SUPPORTED
                    : CapabilityResult.unsupported("case insensitive");
              }
            });
    MetadataObject column =
        MetadataObjects.parse("CAT.SCHEMA.Table.CoL", MetadataObject.Type.COLUMN);
    assertEquals(
        MetadataObjects.parse("CAT.schema.Table.col", MetadataObject.Type.COLUMN),
        MetadataIdConverter.normalizeMetadataObject(column, "Metalake"));
  }

  @Test
  void testColumnNormalizationPreservesSchemaAndColumnCaseIndependently() {
    when(catalog.capability())
        .thenReturn(
            new Capability() {
              @Override
              public CapabilityResult caseSensitiveOnName(Scope scope) {
                return scope == Scope.TABLE
                    ? CapabilityResult.unsupported("case insensitive")
                    : CapabilityResult.SUPPORTED;
              }
            });
    assertEquals(
        MetadataObjects.parse("CAT.SCHEMA.table.CoL", MetadataObject.Type.COLUMN),
        MetadataIdConverter.normalizeMetadataObject(
            MetadataObjects.parse("CAT.SCHEMA.Table.CoL", MetadataObject.Type.COLUMN), "Metalake"));
  }

  @Test
  void testNormalizationIsReusedWithinRequestAndRefreshedOnNextRequest() throws IOException {
    MetadataObject table = object(MetadataObject.Type.TABLE, false);
    put(table, 100L);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(table, METALAKE, context));
    // The loader reuses the normalized name, so even a shared miss resolves capabilities once.
    verify(catalogs, times(1)).doWithCatalog(any(), any());
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(table, METALAKE, context));
    verify(catalogs, times(1)).doWithCatalog(any(), any());
    assertEquals(Optional.of(100L), resolve(table));
    verify(catalogs, times(2)).doWithCatalog(any(), any());
    verify(store, times(1)).get(any(), any(), any());
    doThrow(new NoSuchCatalogException("Missing catalog"))
        .when(catalogs)
        .doWithCatalog(any(), any());
    assertEquals(Optional.empty(), resolve(table));
  }

  @Test
  void testFailedNormalizationCanRetryInSameRequest() {
    MetadataObject table = object(MetadataObject.Type.TABLE, false);
    put(table, 100L);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    doThrow(new NoSuchCatalogException("Missing catalog"))
        .doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    assertEquals(Optional.empty(), lookups.resolveMetadataId(table, METALAKE, context));
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(table, METALAKE, context));
  }

  @Test
  void testLoaderUsesTheCanonicalNameOfItsCacheKey() {
    MetadataObject alias = object(MetadataObject.Type.TABLE, true);
    MetadataObject normalized = object(MetadataObject.Type.TABLE, false);
    put(normalized, 100L);
    put(alias, 200L);
    // A later request can observe different rules; the loader still uses its own canonical key.
    when(catalog.capability()).thenReturn(CASE_INSENSITIVE, Capability.DEFAULT);
    assertEquals(Optional.of(100L), resolve(alias));
    assertEquals(Optional.of(100L), resolve(normalized));
    assertEquals(1L, metadataCache.size());
  }

  @Test
  void testNonCatalogScopesKeepNamesAndSkipCapabilityLookup() {
    for (MetadataObject.Type type :
        List.of(
            MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG, MetadataObject.Type.ROLE)) {
      MetadataObject object = MetadataObjects.of(null, "MixedName", type);
      assertSame(object, MetadataIdConverter.normalizeMetadataObject(object, METALAKE));
    }
    verifyNoInteractions(catalogs);
  }

  @Test
  void testMissingCatalogDoesNotReturnCachedId() {
    MetadataObject table = object(MetadataObject.Type.TABLE, false);
    put(table, 100L);
    assertEquals(Optional.of(100L), resolve(table));
    doThrow(new NoSuchCatalogException("Missing catalog"))
        .when(catalogs)
        .doWithCatalog(any(), any());
    assertEquals(Optional.empty(), resolve(table));
  }

  @Test
  void testCapabilityFailureDoesNotReturnSharedOrRequestCachedId() {
    MetadataObject alias = object(MetadataObject.Type.TABLE, true);
    MetadataObject normalized = object(MetadataObject.Type.TABLE, false);
    put(normalized, 100L);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(normalized, METALAKE, context));
    doThrow(new IllegalStateException("Connector initialization failed"))
        .when(catalogs)
        .doWithCatalog(any(), any());
    // This spelling is new to the request, but its canonical ID is present in both cache tiers.
    assertEquals(Optional.empty(), lookups.resolveMetadataId(alias, METALAKE, context));
    assertEquals(Optional.empty(), resolve(alias));
    assertEquals(1L, metadataCache.size());
  }

  @Test
  void testCapabilityFailureOnCacheMissDoesNotLoadOrCacheId() {
    MetadataObject table = object(MetadataObject.Type.TABLE, true);
    doThrow(new IllegalStateException("Connector initialization failed"))
        .when(catalogs)
        .doWithCatalog(any(), any());
    assertEquals(Optional.empty(), resolve(table));
    assertEquals(0L, metadataCache.size());
    verifyNoInteractions(store);
  }

  @Test
  void testTransientCapabilityFailureCanRetryInSameRequest() throws IOException {
    MetadataObject table = object(MetadataObject.Type.TABLE, true);
    put(object(MetadataObject.Type.TABLE, false), 100L);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    doThrow(new IllegalStateException("Connector initialization failed"))
        .doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    assertEquals(Optional.empty(), lookups.resolveMetadataId(table, METALAKE, context));
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(table, METALAKE, context));
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(table, METALAKE, context));
    verify(catalogs, times(2)).doWithCatalog(any(), any());
    verify(store, times(1)).get(any(), any(), any());
  }

  @Test
  void testEntityStoreFailureStillPropagatesWithCause() throws IOException {
    MetadataObject table = object(MetadataObject.Type.TABLE, true);
    IOException failure = new IOException("Entity store unavailable");
    doThrow(failure).when(store).get(any(), any(), any());
    RuntimeException exception = assertThrows(RuntimeException.class, () -> resolve(table));
    assertSame(failure, exception.getCause());
    assertTrue(exception.getMessage().contains("cat.schema.object"));
    assertEquals(0L, metadataCache.size());
  }

  @Test
  void testViewsAndFunctionsNormalizeNamesLikeTheirDispatchers() {
    // Listed explicitly: the parameterized cases derive from the mapping and would vanish with it.
    for (MetadataObject.Type type :
        List.of(MetadataObject.Type.VIEW, MetadataObject.Type.FUNCTION)) {
      put(object(type, false), 100L);
      assertEquals(
          object(type, false),
          MetadataIdConverter.normalizeMetadataObject(object(type, true), METALAKE));
      assertEquals(Optional.of(100L), resolve(object(type, true)));
    }
  }

  @Test
  void testSemanticModelLeafRemainsCaseSensitiveInInsensitiveCatalog() {
    MetadataObject upper =
        MetadataObjects.parse("cat.SCHEMA.Model", MetadataObject.Type.SEMANTIC_MODEL);
    MetadataObject lower =
        MetadataObjects.parse("cat.schema.model", MetadataObject.Type.SEMANTIC_MODEL);
    put(object(MetadataObject.Type.SEMANTIC_MODEL, false), 100L);
    put(lower, 200L);
    assertEquals(Optional.of(100L), resolve(upper));
    assertEquals(Optional.of(200L), resolve(lower));
    assertEquals(2L, metadataCache.size());
  }

  private void assertAliasesShareBothCacheTiers(MetadataObject alias, MetadataObject normalized)
      throws IOException {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(alias, METALAKE, context));
    assertEquals(Optional.of(100L), lookups.resolveMetadataId(normalized, METALAKE, context));
    assertEquals(Optional.of(100L), resolve(alias));
    assertEquals(1L, metadataCache.size());
    assertTrue(
        metadataCache
            .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, normalized))
            .isPresent());
    assertFalse(
        metadataCache
            .getIfPresent(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, alias))
            .isPresent());
    verify(store, times(1))
        .get(
            ident(normalized),
            entityType(normalized.type()),
            EntityClassMapper.getEntityClass(entityType(normalized.type())));
  }

  private Optional<Long> resolve(MetadataObject object) {
    return lookups.resolveMetadataId(object, METALAKE, new AuthorizationRequestContext());
  }

  private void put(MetadataObject object, long id) {
    Entity entity = mock(EntityClassMapper.getEntityClass(entityType(object.type())));
    when(((HasIdentifier) entity).id()).thenReturn(id);
    entities.put(ident(object), entity);
  }

  private static NameIdentifier ident(MetadataObject object) {
    return MetadataObjectUtil.toEntityIdent(METALAKE, object);
  }

  private static Entity.EntityType entityType(MetadataObject.Type type) {
    return MetadataObjectUtil.toEntityType(type);
  }

  private static EntityChangeRecord change(MetadataObject object, OperateType operation) {
    return new EntityChangeRecord(
        1L,
        METALAKE,
        object.type().name(),
        EntityChangeLogNameIdentifierCodec.encode(ident(object)),
        operation,
        1L);
  }

  private static MetadataObject object(MetadataObject.Type type, boolean alias) {
    String name;
    switch (type) {
      case SCHEMA:
        name = alias ? "cat.SCHEMA" : "cat.schema";
        break;
      case COLUMN:
        name = alias ? "cat.SCHEMA.OBJECT.CoL" : "cat.schema.object.col";
        break;
      case SEMANTIC_MODEL:
        name = alias ? "cat.SCHEMA.Model" : "cat.schema.Model";
        break;
      case TABLE:
      case VIEW:
      case FUNCTION:
      case MODEL:
      case FILESET:
      case TOPIC:
        name = alias ? "cat.SCHEMA.OBJECT" : "cat.schema.object";
        break;
      default:
        throw new IllegalArgumentException("Add normalization coverage for " + type);
    }
    return MetadataObjects.parse(name, type);
  }

  private static Stream<MetadataObject.Type> idLookupTypes() throws IllegalAccessException {
    // Only HasIdentifier entities satisfy EntityStore.get's production contract. COLUMN remains
    // covered by normalization and descendant-key eviction, not by a fabricated ID-load fixture.
    return catalogScopedTypes()
        .filter(
            type ->
                HasIdentifier.class.isAssignableFrom(
                    EntityClassMapper.getEntityClass(entityType(type))));
  }

  private static Stream<MetadataObject.Type> catalogScopedTypes() {
    // Derive coverage from the production registry: a newly mapped type cannot silently be missed.
    return MetadataIdConverter.catalogScopedTypes().stream();
  }
}
