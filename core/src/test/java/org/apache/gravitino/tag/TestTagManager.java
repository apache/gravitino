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
package org.apache.gravitino.tag;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.catalog.TreeLockTestSupport;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.SupportsTableNameResolution;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.RaceTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

public class TestTagManager {

  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final Config config = Mockito.mock(Config.class);

  private static final String METALAKE = "metalake_for_tag_test";

  private static final String CATALOG = "catalog_for_tag_test";

  private static final String SCHEMA = "schema_for_tag_test";

  private static final String TABLE = "table_for_tag_test";

  private static final String COLUMN = "column_for_tag_test";

  private static final String VIEW = "view_for_tag_test";

  private static final String FUNCTION = "function_for_tag_test";

  private static final String SEMANTIC_MODEL = "semantic_model_for_tag_test";

  private static final MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);
  private static final CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
  private static final SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
  private static final TableDispatcher tableDispatcher = mock(TableDispatcher.class);
  private static final ViewDispatcher viewDispatcher = mock(ViewDispatcher.class);
  private static final FunctionDispatcher functionDispatcher = mock(FunctionDispatcher.class);
  private static final SemanticModelDispatcher semanticModelDispatcher =
      mock(SemanticModelDispatcher.class);

  private static EntityStore entityStore;

  private static IdGenerator idGenerator;

  @Nullable private static IdGenerator previousIdGenerator;

  private static TagManager tagManager;

  private static final CatalogManager catalogManager = mock(CatalogManager.class);
  private static final BaseCatalog<?> backendCatalog = mock(BaseCatalog.class);
  private static CatalogManager previousCatalogManager;

  private static final Capability CASE_INSENSITIVE_CAPABILITY =
      new Capability() {
        @Override
        public CapabilityResult caseSensitiveOnName(Scope scope) {
          return CapabilityResult.unsupported("case insensitive");
        }
      };

  @BeforeAll
  public static void setUp() throws IOException, IllegalAccessException {
    idGenerator = new RandomIdGenerator();
    previousIdGenerator = GravitinoEnv.getInstance().idGenerator();
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", idGenerator, true);

    File dbDir = new File(DB_DIR);
    dbDir.mkdirs();

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
    // Fix cache config for test
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
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

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

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(idGenerator.nextId())
            .withName(CATALOG)
            .withNamespace(Namespace.of(METALAKE))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("Test catalog")
            .withAuditInfo(audit)
            .build();
    entityStore.put(catalog, false /* overwritten */);

    SchemaEntity schema =
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(SCHEMA)
            .withNamespace(Namespace.of(METALAKE, CATALOG))
            .withComment("Test schema")
            .withAuditInfo(audit)
            .build();
    entityStore.put(schema, false /* overwritten */);

    ColumnEntity column =
        ColumnEntity.builder()
            .withId(idGenerator.nextId())
            .withName(COLUMN)
            .withPosition(0)
            .withComment("Test column")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(audit)
            .build();

    TableEntity table =
        TableEntity.builder()
            .withId(idGenerator.nextId())
            .withName(TABLE)
            .withColumns(Lists.newArrayList(column))
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withAuditInfo(audit)
            .build();
    entityStore.put(table, false /* overwritten */);

    ViewEntity view =
        ViewEntity.builder()
            .withId(idGenerator.nextId())
            .withName(VIEW)
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withColumns(new Column[0])
            .withRepresentations(
                new Representation[] {
                  SQLRepresentation.builder().withDialect("unknown").withSql("SELECT 1").build()
                })
            .withAuditInfo(audit)
            .build();
    entityStore.put(view, false /* overwritten */);

    FunctionParam param = FunctionParams.of("param", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofJava(FunctionImpl.RuntimeType.SPARK, "mock.udf.class.name");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});
    FunctionEntity function =
        FunctionEntity.builder()
            .withId(idGenerator.nextId())
            .withName(FUNCTION)
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(false)
            .withDefinitions(new FunctionDefinition[] {definition})
            .withAuditInfo(audit)
            .build();
    entityStore.put(function, false /* overwritten */);

    tagManager = new TagManager(idGenerator, entityStore);
    previousCatalogManager =
        (CatalogManager) FieldUtils.readField(GravitinoEnv.getInstance(), "catalogManager", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, backendCatalog);
    when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);

    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalMetalakeDispatcher", metalakeDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalCatalogDispatcher", catalogDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalSchemaDispatcher", schemaDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalTableDispatcher", tableDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalViewDispatcher", viewDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalFunctionDispatcher", functionDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "internalSemanticModelDispatcher",
        semanticModelDispatcher,
        true);

    when(metalakeDispatcher.metalakeExists(any())).thenReturn(true);
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);
    when(schemaDispatcher.schemaExists(any())).thenReturn(true);
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    when(viewDispatcher.viewExists(any())).thenReturn(true);
    when(functionDispatcher.functionExists(any())).thenReturn(true);
  }

  @AfterAll
  public static void tearDown() throws IOException, IllegalAccessException {
    try {
      if (entityStore != null) {
        entityStore.close();
        entityStore = null;
      }

      FileUtils.deleteDirectory(new File(JDBC_STORE_PATH));
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", previousIdGenerator, true);
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "catalogManager", previousCatalogManager, true);
    }
  }

  @AfterEach
  public void cleanUp() {
    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    String[] catalogTags = tagManager.listTagsForMetadataObject(METALAKE, catalogObject);
    tagManager.associateTagsForMetadataObject(METALAKE, catalogObject, null, catalogTags);

    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    String[] schemaTags = tagManager.listTagsForMetadataObject(METALAKE, schemaObject);
    tagManager.associateTagsForMetadataObject(METALAKE, schemaObject, null, schemaTags);

    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    String[] tableTags = tagManager.listTagsForMetadataObject(METALAKE, tableObject);
    tagManager.associateTagsForMetadataObject(METALAKE, tableObject, null, tableTags);

    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);
    String[] columnTags = tagManager.listTagsForMetadataObject(METALAKE, columnObject);
    tagManager.associateTagsForMetadataObject(METALAKE, columnObject, null, columnTags);

    MetadataObject viewObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofView(METALAKE, CATALOG, SCHEMA, VIEW), Entity.EntityType.VIEW);
    String[] viewTags = tagManager.listTagsForMetadataObject(METALAKE, viewObject);
    tagManager.associateTagsForMetadataObject(METALAKE, viewObject, null, viewTags);

    MetadataObject functionObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofFunction(METALAKE, CATALOG, SCHEMA, FUNCTION),
            Entity.EntityType.FUNCTION);
    String[] functionTags = tagManager.listTagsForMetadataObject(METALAKE, functionObject);
    tagManager.associateTagsForMetadataObject(METALAKE, functionObject, null, functionTags);

    Arrays.stream(tagManager.listTags(METALAKE)).forEach(n -> tagManager.deleteTag(METALAKE, n));
  }

  @ParameterizedTest
  @EnumSource(
      value = Entity.EntityType.class,
      names = {"SCHEMA", "TABLE", "COLUMN", "VIEW", "FUNCTION"})
  void testTagOperationsNormalizeCaseInsensitiveNames(Entity.EntityType type) {
    MetadataObject canonical = tagTarget(type, false);
    MetadataObject request = tagTarget(type, true);
    when(backendCatalog.capability()).thenReturn(CASE_INSENSITIVE_CAPABILITY);
    try {
      assertTagRoundTrip(request, canonical, canonical);
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = Entity.EntityType.class,
      names = {"FILESET", "TOPIC", "MODEL"})
  void testTagOperationsNormalizeOtherCatalogObjects(Entity.EntityType type)
      throws IOException, IllegalAccessException {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, SCHEMA, "normalization_object");
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    Object dispatcher;
    String dispatcherField;
    switch (type) {
      case FILESET:
        entityStore.put(
            FilesetEntity.builder()
                .withId(idGenerator.nextId())
                .withName(ident.name())
                .withNamespace(ident.namespace())
                .withFilesetType(Fileset.Type.EXTERNAL)
                .withStorageLocations(Map.of("default", "file:/tmp/normalization_object"))
                .withAuditInfo(audit)
                .build(),
            false);
        FilesetDispatcher filesetDispatcher = mock(FilesetDispatcher.class);
        when(filesetDispatcher.filesetExists(any())).thenReturn(true);
        dispatcher = filesetDispatcher;
        dispatcherField = "internalFilesetDispatcher";
        break;
      case TOPIC:
        entityStore.put(
            TopicEntity.builder()
                .withId(idGenerator.nextId())
                .withName(ident.name())
                .withNamespace(ident.namespace())
                .withAuditInfo(audit)
                .build(),
            false);
        TopicDispatcher topicDispatcher = mock(TopicDispatcher.class);
        when(topicDispatcher.topicExists(any())).thenReturn(true);
        dispatcher = topicDispatcher;
        dispatcherField = "internalTopicDispatcher";
        break;
      case MODEL:
        entityStore.put(
            ModelEntity.builder()
                .withId(idGenerator.nextId())
                .withName(ident.name())
                .withNamespace(ident.namespace())
                .withLatestVersion(0)
                .withAuditInfo(audit)
                .build(),
            false);
        ModelDispatcher modelDispatcher = mock(ModelDispatcher.class);
        when(modelDispatcher.modelExists(any())).thenReturn(true);
        dispatcher = modelDispatcher;
        dispatcherField = "internalModelDispatcher";
        break;
      default:
        throw new IllegalArgumentException("Unsupported test target: " + type);
    }
    Object previousDispatcher =
        FieldUtils.readField(GravitinoEnv.getInstance(), dispatcherField, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), dispatcherField, dispatcher, true);
    when(backendCatalog.capability()).thenReturn(CASE_INSENSITIVE_CAPABILITY);
    try {
      MetadataObject request =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(
                  METALAKE,
                  CATALOG,
                  SCHEMA.toUpperCase(Locale.ROOT),
                  ident.name().toUpperCase(Locale.ROOT)),
              type);
      MetadataObject canonical = NameIdentifierUtil.toMetadataObject(ident, type);
      assertTagRoundTrip(request, canonical, canonical);
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
      FieldUtils.writeField(GravitinoEnv.getInstance(), dispatcherField, previousDispatcher, true);
      entityStore.delete(ident, type);
    }
  }

  @Test
  void testCatalogAndMetalakeTagOperationsDoNotLoadCapabilities() {
    Mockito.clearInvocations(catalogManager);
    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifier.of(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    assertTagRoundTrip(catalogObject, catalogObject, catalogObject);
    MetadataObject metalakeObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifier.of(METALAKE), Entity.EntityType.METALAKE);
    // Metalake tags are unsupported: preserve the existing store failure without loading a catalog.
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> tagManager.listTagsForMetadataObject(METALAKE, metalakeObject));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tagManager.associateTagsForMetadataObject(
                METALAKE, metalakeObject, new String[] {"normalization_tag"}, null));
    Mockito.verifyNoInteractions(catalogManager);
  }

  @Test
  void testTagOperationsPreserveCaseSensitiveNames() {
    MetadataObject canonical = tagTarget(Entity.EntityType.TABLE, false);
    MetadataObject differentlyCased = tagTarget(Entity.EntityType.TABLE, true);
    Tag tag = tagManager.createTag(METALAKE, "case_sensitive_tag", null, null);
    tagManager.associateTagsForMetadataObject(METALAKE, canonical, new String[] {tag.name()}, null);

    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> tagManager.listTagsForMetadataObject(METALAKE, differentlyCased));
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> tagManager.getTagForMetadataObject(METALAKE, differentlyCased, tag.name()));
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            tagManager.associateTagsForMetadataObject(
                METALAKE, differentlyCased, null, new String[] {tag.name()}));
    Assertions.assertArrayEquals(
        new String[] {tag.name()}, tagManager.listTagsForMetadataObject(METALAKE, canonical));
  }

  @Test
  void testTagOperationsPreserveQuotedNamesAndOriginalExistenceCheck() {
    Capability quoteAware =
        new Capability() {
          @Override
          public String normalizeName(Scope scope, String name) {
            return name.startsWith("\"") && name.endsWith("\"")
                ? name.substring(1, name.length() - 1)
                : name.toUpperCase(Locale.ROOT);
          }
        };
    NameIdentifier quotedTable =
        NameIdentifier.of(METALAKE, CATALOG, "\"" + SCHEMA + "\"", "\"" + TABLE + "\"");
    MetadataObject request =
        NameIdentifierUtil.toMetadataObject(quotedTable, Entity.EntityType.TABLE);
    when(backendCatalog.capability()).thenReturn(quoteAware);
    when(tableDispatcher.tableExists(any()))
        .thenAnswer(invocation -> quotedTable.equals(invocation.getArgument(0)));
    try {
      assertTagRoundTrip(request, request, tagTarget(Entity.EntityType.TABLE, false));
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
      when(tableDispatcher.tableExists(any())).thenReturn(true);
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = Entity.EntityType.class,
      names = {"TABLE", "COLUMN"})
  void testTagOperationsResolvePhysicalTableName(Entity.EntityType type) {
    CatalogOperations ops =
        mock(
            CatalogOperations.class,
            Mockito.withSettings().extraInterfaces(SupportsTableNameResolution.class));
    Mockito.doReturn(ops).when(backendCatalog).ops();
    when(((SupportsTableNameResolution) ops).resolveTableName(any()))
        .thenAnswer(
            invocation -> {
              NameIdentifier ident = invocation.getArgument(0);
              return ident.name().equals("table_alias")
                  ? NameIdentifier.of(ident.namespace(), TABLE)
                  : ident;
            });
    when(backendCatalog.capability()).thenReturn(CASE_INSENSITIVE_CAPABILITY);
    NameIdentifier alias =
        type == Entity.EntityType.TABLE
            ? NameIdentifier.of(METALAKE, CATALOG, SCHEMA.toUpperCase(Locale.ROOT), "TABLE_ALIAS")
            : NameIdentifier.of(
                METALAKE,
                CATALOG,
                SCHEMA.toUpperCase(Locale.ROOT),
                "TABLE_ALIAS",
                COLUMN.toUpperCase(Locale.ROOT));
    MetadataObject canonical = tagTarget(type, false);
    try {
      assertTagRoundTrip(NameIdentifierUtil.toMetadataObject(alias, type), canonical, canonical);
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
      Mockito.doReturn(null).when(backendCatalog).ops();
    }
  }

  @Test
  void testTagOperationsFollowGlueStyleColumnCase() throws IOException {
    // Glue folds schema and table names but keeps the case of column names.
    NameIdentifier table = putTableWithColumn("glue_table", "MixedCol");
    Capability glueStyle =
        new Capability() {
          @Override
          public CapabilityResult caseSensitiveOnName(Scope scope) {
            return scope == Scope.SCHEMA || scope == Scope.TABLE
                ? CapabilityResult.unsupported("case insensitive")
                : CapabilityResult.SUPPORTED;
          }
        };
    when(backendCatalog.capability()).thenReturn(glueStyle);
    try {
      MetadataObject request =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(
                  METALAKE, CATALOG, SCHEMA.toUpperCase(Locale.ROOT), "GLUE_TABLE", "MixedCol"),
              Entity.EntityType.COLUMN);
      MetadataObject canonical =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(METALAKE, CATALOG, SCHEMA, table.name(), "MixedCol"),
              Entity.EntityType.COLUMN);
      assertTagRoundTrip(request, canonical, canonical);
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
      entityStore.delete(table, Entity.EntityType.TABLE);
    }
  }

  @Test
  void testTagOperationsKeepExactStoredColumnName() throws IOException {
    // A capability can fold column names that the source stored with another case. The exact
    // stored name must still find the column.
    NameIdentifier table = putTableWithColumn("exact_table", "MyCol");
    when(backendCatalog.capability()).thenReturn(CASE_INSENSITIVE_CAPABILITY);
    try {
      MetadataObject canonical =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(METALAKE, CATALOG, SCHEMA, table.name(), "MyCol"),
              Entity.EntityType.COLUMN);
      MetadataObject request =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(
                  METALAKE, CATALOG, SCHEMA.toUpperCase(Locale.ROOT), "EXACT_TABLE", "MyCol"),
              Entity.EntityType.COLUMN);
      assertTagRoundTrip(request, canonical, canonical);
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
      entityStore.delete(table, Entity.EntityType.TABLE);
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = Entity.EntityType.class,
      names = {"TABLE", "COLUMN"})
  void testFailedTableNameResolutionLeavesTagsUnchanged(Entity.EntityType type) {
    MetadataObject canonical = tagTarget(type, false);
    Tag kept = tagManager.createTag(METALAKE, "kept_tag", null, null);
    Tag added = tagManager.createTag(METALAKE, "added_tag", null, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, canonical, new String[] {kept.name()}, null);

    CatalogOperations ops =
        mock(
            CatalogOperations.class,
            Mockito.withSettings().extraInterfaces(SupportsTableNameResolution.class));
    when(((SupportsTableNameResolution) ops).resolveTableName(any()))
        .thenThrow(new IllegalStateException("source unavailable"));
    Mockito.doReturn(ops).when(backendCatalog).ops();
    try {
      RuntimeException addError =
          Assertions.assertThrows(
              RuntimeException.class,
              () ->
                  tagManager.associateTagsForMetadataObject(
                      METALAKE, canonical, new String[] {added.name()}, null));
      Assertions.assertInstanceOf(IllegalStateException.class, addError.getCause());
      Assertions.assertThrows(
          RuntimeException.class,
          () ->
              tagManager.associateTagsForMetadataObject(
                  METALAKE, canonical, null, new String[] {kept.name()}));
      Assertions.assertThrows(
          RuntimeException.class,
          () ->
              tagManager.associateTagValuesForMetadataObject(
                  METALAKE, canonical, new TagValue[] {TagValue.of(added.name(), "v")}, null));
    } finally {
      Mockito.doReturn(null).when(backendCatalog).ops();
    }
    Assertions.assertArrayEquals(
        new String[] {kept.name()}, tagManager.listTagsForMetadataObject(METALAKE, canonical));
  }

  @Test
  void testTagOperationsUseCustomNormalization() throws IOException {
    NameIdentifier uppercase = NameIdentifier.of(METALAKE, CATALOG, SCHEMA, "CUSTOM_TABLE");
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    entityStore.put(
        TableEntity.builder()
            .withId(idGenerator.nextId())
            .withName(uppercase.name())
            .withNamespace(uppercase.namespace())
            .withColumns(Lists.newArrayList())
            .withAuditInfo(audit)
            .build(),
        false);
    Capability uppercaseTables =
        new Capability() {
          @Override
          public String normalizeName(Scope scope, String name) {
            return scope == Scope.TABLE ? name.toUpperCase(Locale.ROOT) : name;
          }
        };
    when(backendCatalog.capability()).thenReturn(uppercaseTables);
    try {
      MetadataObject request =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(uppercase.namespace(), "custom_table"), Entity.EntityType.TABLE);
      MetadataObject canonical =
          NameIdentifierUtil.toMetadataObject(uppercase, Entity.EntityType.TABLE);
      assertTagRoundTrip(request, canonical, canonical);
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
      entityStore.delete(uppercase, Entity.EntityType.TABLE);
    }
  }

  @Test
  void testTagOperationsPreserveSemanticModelName() throws IOException {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, SCHEMA, "SalesModel");
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    Dataset dataset =
        Dataset.builder()
            .withName("sales")
            .withSource(NameIdentifier.of(CATALOG, SCHEMA, TABLE))
            .build();
    entityStore.put(
        SemanticModelEntity.builder()
            .withId(idGenerator.nextId())
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withAuditInfo(audit)
            .withDefinition(
                SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build())
            .build(),
        false);
    when(backendCatalog.capability()).thenReturn(CASE_INSENSITIVE_CAPABILITY);
    when(semanticModelDispatcher.semanticModelExists(any())).thenReturn(true);
    try {
      MetadataObject request =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(METALAKE, CATALOG, SCHEMA.toUpperCase(Locale.ROOT), ident.name()),
              Entity.EntityType.SEMANTIC_MODEL);
      MetadataObject canonical =
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SEMANTIC_MODEL);
      assertTagRoundTrip(request, canonical, canonical);
      MetadataObject wrongCase =
          NameIdentifierUtil.toMetadataObject(
              NameIdentifier.of(ident.namespace(), "salesmodel"), Entity.EntityType.SEMANTIC_MODEL);
      Assertions.assertThrows(
          NoSuchMetadataObjectException.class,
          () -> tagManager.listTagsForMetadataObject(METALAKE, wrongCase));
    } finally {
      when(backendCatalog.capability()).thenReturn(Capability.DEFAULT);
      when(semanticModelDispatcher.semanticModelExists(any())).thenReturn(false);
      entityStore.delete(ident, Entity.EntityType.SEMANTIC_MODEL);
    }
  }

  @Test
  public void testCreateAndGetTag() {
    Tag tag = tagManager.createTag(METALAKE, "tag1", null, null);
    Assertions.assertEquals("tag1", tag.name());
    Assertions.assertNull(tag.comment());
    Assertions.assertTrue(tag.properties().isEmpty());

    Tag tag1 = tagManager.getTag(METALAKE, "tag1");
    Assertions.assertEquals(tag, tag1);

    // Create a tag in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> tagManager.createTag("non_existent_metalake", "tag1", null, null));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());

    // Create a existent tag
    e =
        Assertions.assertThrows(
            TagAlreadyExistsException.class,
            () -> tagManager.createTag(METALAKE, "tag1", null, null));
    Assertions.assertEquals(
        "Tag with name tag1 under metalake metalake_for_tag_test already exists", e.getMessage());

    // Get a non-existent tag
    e =
        Assertions.assertThrows(
            NoSuchTagException.class, () -> tagManager.getTag(METALAKE, "non_existent_tag"));
    Assertions.assertEquals(
        "Tag with name non_existent_tag under metalake metalake_for_tag_test does not exist",
        e.getMessage());
  }

  @Test
  public void testCreateAndListTags() {
    tagManager.createTag(METALAKE, "tag1", null, null);
    tagManager.createTag(METALAKE, "tag2", null, null);
    tagManager.createTag(METALAKE, "tag3", null, null);

    Set<String> tagNames = Arrays.stream(tagManager.listTags(METALAKE)).collect(Collectors.toSet());
    Assertions.assertEquals(3, tagNames.size());
    Set<String> expectedNames = ImmutableSet.of("tag1", "tag2", "tag3");
    Assertions.assertEquals(expectedNames, tagNames);

    // List tags in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> tagManager.listTags("non_existent_metalake"));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());
  }

  /** Invalid create/rename requests must never leave a persisted dotted name. */
  @Test
  public void testRejectDottedNameBeforeWrite() throws IOException {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> tagManager.createTag(METALAKE, "invalid.tag", null, Map.of()));
    Assertions.assertFalse(
        entityStore.exists(
            NameIdentifierUtil.ofTag(METALAKE, "invalid.tag"), Entity.EntityType.TAG));
    Tag original = tagManager.createTag(METALAKE, "valid_tag", null, Map.of());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> tagManager.alterTag(METALAKE, original.name(), TagChange.rename("invalid.tag")));
    Assertions.assertEquals(original, tagManager.getTag(METALAKE, original.name()));
    Assertions.assertFalse(
        entityStore.exists(
            NameIdentifierUtil.ofTag(METALAKE, "invalid.tag"), Entity.EntityType.TAG));
  }

  /** A legacy dotted tag remains readable and removable through the manager. */
  @Test
  public void testLegacyDottedTagRecovery() throws IOException {
    TagEntity legacy =
        TagEntity.builder()
            .withId(idGenerator.nextId())
            .withName("legacy.tag")
            .withNamespace(NameIdentifierUtil.ofTag(METALAKE, "legacy.tag").namespace())
            .withProperties(Map.of())
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();
    entityStore.put(legacy, false);
    Assertions.assertEquals(legacy.name(), tagManager.getTag(METALAKE, legacy.name()).name());
    Assertions.assertTrue(Arrays.asList(tagManager.listTags(METALAKE)).contains(legacy.name()));
    Assertions.assertTrue(tagManager.deleteTag(METALAKE, legacy.name()));
  }

  @Test
  public void testAlterTag() {
    String tagComment = "tag comment";
    Map<String, String> tagProp = ImmutableMap.of("k1", "k2");
    tagManager.createTag(METALAKE, "tag1", tagComment, tagProp);

    // Test rename tag
    TagChange rename = TagChange.rename("new_tag1");
    Tag renamedTag = tagManager.alterTag(METALAKE, "tag1", rename);
    Assertions.assertEquals("new_tag1", renamedTag.name());
    Assertions.assertEquals(tagComment, renamedTag.comment());
    Assertions.assertEquals(tagProp, renamedTag.properties());

    // Test change comment
    TagChange changeComment = TagChange.updateComment("new comment");
    Tag changedCommentTag = tagManager.alterTag(METALAKE, "new_tag1", changeComment);
    Assertions.assertEquals("new_tag1", changedCommentTag.name());
    Assertions.assertEquals("new comment", changedCommentTag.comment());
    Assertions.assertEquals(tagProp, changedCommentTag.properties());

    // Test add new property
    TagChange addProp = TagChange.setProperty("k2", "v2");
    Tag addedPropTag = tagManager.alterTag(METALAKE, "new_tag1", addProp);
    Assertions.assertEquals("new_tag1", addedPropTag.name());
    Assertions.assertEquals("new comment", addedPropTag.comment());
    Map<String, String> expectedProp = ImmutableMap.of("k1", "k2", "k2", "v2");
    Assertions.assertEquals(expectedProp, addedPropTag.properties());

    // Test update existing property
    TagChange updateProp = TagChange.setProperty("k1", "v1");
    Tag updatedPropTag = tagManager.alterTag(METALAKE, "new_tag1", updateProp);
    Assertions.assertEquals("new_tag1", updatedPropTag.name());
    Assertions.assertEquals("new comment", updatedPropTag.comment());
    Map<String, String> expectedProp1 = ImmutableMap.of("k1", "v1", "k2", "v2");
    Assertions.assertEquals(expectedProp1, updatedPropTag.properties());

    // Test remove property
    TagChange removeProp = TagChange.removeProperty("k1");
    Tag removedPropTag = tagManager.alterTag(METALAKE, "new_tag1", removeProp);
    Assertions.assertEquals("new_tag1", removedPropTag.name());
    Assertions.assertEquals("new comment", removedPropTag.comment());
    Map<String, String> expectedProp2 = ImmutableMap.of("k2", "v2");
    Assertions.assertEquals(expectedProp2, removedPropTag.properties());
  }

  @Test
  public void testTagNameAndCommentLength() {
    String maxLengthName = StringUtils.repeat("a", EntityFieldLimits.MAX_NAME_LENGTH);
    String tooLongName = maxLengthName + "a";
    String maxLengthComment = StringUtils.repeat("c", EntityFieldLimits.MAX_COMMENT_LENGTH);
    String tooLongComment = maxLengthComment + "c";

    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tagManager.createTag(METALAKE, tooLongName, null, null));
    Assertions.assertEquals("The name of the tag must not exceed 128 characters", e.getMessage());

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tagManager.createTag(METALAKE, "tag1", tooLongComment, null));
    Assertions.assertEquals(
        "The comment of the tag must not exceed 256 characters", e.getMessage());

    Tag tag = tagManager.createTag(METALAKE, maxLengthName, maxLengthComment, null);
    Assertions.assertEquals(maxLengthName, tag.name());
    Assertions.assertEquals(maxLengthComment, tag.comment());

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tagManager.alterTag(METALAKE, maxLengthName, TagChange.rename(tooLongName)));
    Assertions.assertEquals("The name of the tag must not exceed 128 characters", e.getMessage());

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tagManager.alterTag(
                    METALAKE, maxLengthName, TagChange.updateComment(tooLongComment)));
    Assertions.assertEquals(
        "The comment of the tag must not exceed 256 characters", e.getMessage());

    Tag unchanged = tagManager.getTag(METALAKE, maxLengthName);
    Assertions.assertEquals(maxLengthComment, unchanged.comment());
  }

  @Test
  public void testAlterTagRenameToExistingTag() {
    tagManager.createTag(METALAKE, "tag1", null, null);
    tagManager.createTag(METALAKE, "tag2", null, null);

    TagAlreadyExistsException exception =
        Assertions.assertThrows(
            TagAlreadyExistsException.class,
            () -> tagManager.alterTag(METALAKE, "tag1", TagChange.rename("tag2")));

    Assertions.assertEquals(
        "Tag with name tag2 under metalake metalake_for_tag_test already exists",
        exception.getMessage());

    Assertions.assertEquals("tag1", tagManager.getTag(METALAKE, "tag1").name());
    Assertions.assertEquals("tag2", tagManager.getTag(METALAKE, "tag2").name());
  }

  @Test
  void testAlterTagMultipleRenamesToExistingTag() {
    Tag source = tagManager.createTag(METALAKE, "tag1", "source", ImmutableMap.of("key", "source"));
    Tag target = tagManager.createTag(METALAKE, "tag2", "target", ImmutableMap.of("key", "target"));

    TagAlreadyExistsException exception =
        Assertions.assertThrows(
            TagAlreadyExistsException.class,
            () ->
                tagManager.alterTag(
                    METALAKE,
                    "tag1",
                    TagChange.rename("intermediate"),
                    TagChange.updateComment("changed"),
                    TagChange.setProperty("key", "changed"),
                    TagChange.rename("tag2")));

    Assertions.assertEquals(
        "Tag with name tag2 under metalake metalake_for_tag_test already exists",
        exception.getMessage());
    Assertions.assertInstanceOf(EntityAlreadyExistsException.class, exception.getCause());
    for (Tag original : new Tag[] {source, target}) {
      Tag unchanged = tagManager.getTag(METALAKE, original.name());
      Assertions.assertEquals(original.name(), unchanged.name());
      Assertions.assertEquals(original.comment(), unchanged.comment());
      Assertions.assertEquals(original.properties(), unchanged.properties());
    }
    Assertions.assertThrows(
        NoSuchTagException.class, () -> tagManager.getTag(METALAKE, "intermediate"));
  }

  @Test
  public void testDeleteTag() {
    tagManager.createTag(METALAKE, "tag1", null, null);
    Assertions.assertTrue(tagManager.deleteTag(METALAKE, "tag1"));

    // Delete a non-existent tag
    Assertions.assertFalse(tagManager.deleteTag(METALAKE, "non_existent_tag"));

    // Delete a tag in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> tagManager.deleteTag("non_existent_metalake", "tag1"));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());
  }

  @Test
  public void testAssociateTagsForMetadataObject() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    // Test associate tags for catalog
    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    String[] tagsToAdd = new String[] {tag1.name(), tag2.name(), tag3.name()};

    String[] tags =
        tagManager.associateTagsForMetadataObject(METALAKE, catalogObject, tagsToAdd, null);

    Assertions.assertEquals(3, tags.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2", "tag3"), ImmutableSet.copyOf(tags));

    // Test disassociate tags for catalog
    String[] tagsToRemove = new String[] {tag1.name()};
    String[] tags1 =
        tagManager.associateTagsForMetadataObject(METALAKE, catalogObject, null, tagsToRemove);

    Assertions.assertEquals(2, tags1.length);
    Assertions.assertEquals(ImmutableSet.of("tag2", "tag3"), ImmutableSet.copyOf(tags1));

    // Test associate and disassociate no tags for catalog
    String[] tags2 =
        tagManager.associateTagsForMetadataObject(
            METALAKE, catalogObject, (String[]) null, (String[]) null);

    Assertions.assertEquals(2, tags2.length);
    Assertions.assertEquals(ImmutableSet.of("tag2", "tag3"), ImmutableSet.copyOf(tags2));

    // Test re-associate tags for catalog through the compatibility API.
    Throwable e =
        Assertions.assertThrows(
            TagAlreadyAssociatedException.class,
            () ->
                tagManager.associateTagsForMetadataObject(
                    METALAKE, catalogObject, tagsToAdd, null));
    Assertions.assertTrue(e.getMessage().contains("Failed to associate tags for metadata object"));

    // Test associate and disassociate non-existent tags for catalog
    String[] tagsAfterMissingUpdate =
        tagManager.associateTagsForMetadataObject(
            METALAKE, catalogObject, new String[] {"tag4", "tag5"}, new String[] {"tag6"});

    Assertions.assertEquals(2, tagsAfterMissingUpdate.length);
    Assertions.assertEquals(
        ImmutableSet.of("tag2", "tag3"), ImmutableSet.copyOf(tagsAfterMissingUpdate));

    // Test associate tags for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e1 =
        Assertions.assertThrows(
            NotFoundException.class,
            () ->
                tagManager.associateTagsForMetadataObject(
                    METALAKE, nonExistentObject, tagsToAdd, null));
    Assertions.assertTrue(e1.getMessage().contains("Failed to associate tags for metadata object"));

    // Test associate tags for unsupported metadata object
    MetadataObject metalakeObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofMetalake(METALAKE), Entity.EntityType.METALAKE);
    Throwable e2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tagManager.associateTagsForMetadataObject(
                    METALAKE, metalakeObject, tagsToAdd, null));
    Assertions.assertTrue(
        e2.getMessage().contains("Cannot associate tags for unsupported metadata object type"));

    // Test associate tags for schema
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    String[] tags4 =
        tagManager.associateTagsForMetadataObject(METALAKE, schemaObject, tagsToAdd, null);

    Assertions.assertEquals(3, tags4.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2", "tag3"), ImmutableSet.copyOf(tags4));

    // Test associate tags for table
    String[] tagsToAdd1 = new String[] {tag1.name()};
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    String[] tags5 =
        tagManager.associateTagsForMetadataObject(METALAKE, tableObject, tagsToAdd1, null);

    Assertions.assertEquals(1, tags5.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags5));

    // Test associate and disassociate same tags for table
    String[] tagsToAdd2 = new String[] {tag2.name(), tag3.name()};
    String[] tagsToRemove1 = new String[] {tag2.name()};
    String[] tags6 =
        tagManager.associateTagsForMetadataObject(METALAKE, tableObject, tagsToAdd2, tagsToRemove1);

    Assertions.assertEquals(2, tags6.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag3"), ImmutableSet.copyOf(tags6));

    // Test associate tags for view
    MetadataObject viewObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofView(METALAKE, CATALOG, SCHEMA, VIEW), Entity.EntityType.VIEW);
    String[] tagsForView =
        tagManager.associateTagsForMetadataObject(METALAKE, viewObject, tagsToAdd1, null);

    Assertions.assertEquals(1, tagsForView.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tagsForView));

    // Test associate and disassociate same tags for view
    String[] tagsForViewAfterUpdate =
        tagManager.associateTagsForMetadataObject(METALAKE, viewObject, tagsToAdd2, tagsToRemove1);

    Assertions.assertEquals(2, tagsForViewAfterUpdate.length);
    Assertions.assertEquals(
        ImmutableSet.of("tag1", "tag3"), ImmutableSet.copyOf(tagsForViewAfterUpdate));

    // Test associate tags for function
    MetadataObject functionObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofFunction(METALAKE, CATALOG, SCHEMA, FUNCTION),
            Entity.EntityType.FUNCTION);
    String[] tagsForFunction =
        tagManager.associateTagsForMetadataObject(METALAKE, functionObject, tagsToAdd1, null);

    Assertions.assertEquals(1, tagsForFunction.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tagsForFunction));

    // Test associate and disassociate same tags for function
    String[] tagsForFunctionAfterUpdate =
        tagManager.associateTagsForMetadataObject(
            METALAKE, functionObject, tagsToAdd2, tagsToRemove1);

    Assertions.assertEquals(2, tagsForFunctionAfterUpdate.length);
    Assertions.assertEquals(
        ImmutableSet.of("tag1", "tag3"), ImmutableSet.copyOf(tagsForFunctionAfterUpdate));

    // Test associate and disassociate same tags for column
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);

    String[] tagsToAdd3 = new String[] {tag1.name()};
    String[] tags7 =
        tagManager.associateTagsForMetadataObject(METALAKE, columnObject, tagsToAdd3, null);

    Assertions.assertEquals(1, tags7.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags7));

    // Test associate and disassociate tags for column
    String[] tagsToRemove2 = new String[] {tag1.name()};
    String[] tags8 =
        tagManager.associateTagsForMetadataObject(METALAKE, columnObject, null, tagsToRemove2);

    Assertions.assertEquals(0, tags8.length);
    Assertions.assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(tags8));

    // Test associate and disassociate same tags for column
    String[] tagsToAdd4 = new String[] {tag2.name(), tag3.name()};
    String[] tagsToRemove3 = new String[] {tag2.name()};
    String[] tags9 =
        tagManager.associateTagsForMetadataObject(
            METALAKE, columnObject, tagsToAdd4, tagsToRemove3);

    Assertions.assertEquals(1, tags9.length);
    Assertions.assertEquals(ImmutableSet.of("tag3"), ImmutableSet.copyOf(tags9));
  }

  @Test
  public void testAssociateTagValuesForMetadataObject() {
    Tag tag =
        tagManager.createTag(
            METALAKE,
            "data_domain",
            null,
            null,
            TagValueConstraint.ofAllowedValues("finance", "risk", "finance"));
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    String[] tags =
        tagManager.associateTagValuesForMetadataObject(
            METALAKE,
            tableObject,
            new TagValue[] {TagValue.of(tag.name(), "finance"), TagValue.of(tag.name(), "risk")},
            null);

    Assertions.assertEquals(1, tags.length);
    Assertions.assertEquals(ImmutableSet.of("data_domain"), ImmutableSet.copyOf(tags));

    Tag[] tagInfos = tagManager.listTagsInfoForMetadataObject(METALAKE, tableObject);
    Assertions.assertEquals(1, tagInfos.length);
    Assertions.assertTrue(tagInfos[0].assignment().isPresent());
    Assertions.assertArrayEquals(
        new String[] {"finance", "risk"}, tagInfos[0].assignment().get().values());

    String[] repeatedTags =
        tagManager.associateTagValuesForMetadataObject(
            METALAKE, tableObject, new TagValue[] {TagValue.of(tag.name(), "finance")}, null);
    Assertions.assertArrayEquals(new String[] {tag.name()}, repeatedTags);
    Tag[] repeatedTagInfos = tagManager.listTagsInfoForMetadataObject(METALAKE, tableObject);
    Assertions.assertArrayEquals(
        new String[] {"finance", "risk"}, repeatedTagInfos[0].assignment().get().values());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tagManager.associateTagValuesForMetadataObject(
                METALAKE, tableObject, new TagValue[] {TagValue.of(tag.name(), "pii")}, null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tagManager.associateTagValuesForMetadataObject(
                METALAKE, tableObject, new TagValue[] {TagValue.noValue(tag.name())}, null));

    MetadataObject[] financeObjects =
        tagManager.listMetadataObjectsForTag(METALAKE, tag.name(), "finance");
    Assertions.assertArrayEquals(new MetadataObject[] {tableObject}, financeObjects);
    Assertions.assertEquals(
        0, tagManager.listMetadataObjectsForTag(METALAKE, tag.name(), "pii").length);

    tagManager.associateTagValuesForMetadataObject(
        METALAKE, tableObject, null, new TagValue[] {TagValue.noValue(tag.name())});
    Tag dataDomainInfo = tagManager.getTagForMetadataObject(METALAKE, tableObject, tag.name());
    Assertions.assertArrayEquals(
        new String[] {"finance", "risk"}, dataDomainInfo.assignment().get().values());

    Assertions.assertArrayEquals(
        new MetadataObject[] {tableObject},
        tagManager.listMetadataObjectsForTag(METALAKE, tag.name(), "finance"));

    Tag ownerTag = tagManager.createTag(METALAKE, "owner", null, null);
    tagManager.associateTagValuesForMetadataObject(
        METALAKE, tableObject, new TagValue[] {TagValue.noValue(ownerTag.name())}, null);
    Tag ownerInfoWithoutValue =
        tagManager.getTagForMetadataObject(METALAKE, tableObject, ownerTag.name());
    assertAssignmentValues(ownerInfoWithoutValue, new String[0]);
    tagManager.associateTagValuesForMetadataObject(
        METALAKE, tableObject, new TagValue[] {TagValue.of(ownerTag.name(), "team-a")}, null);
    Tag ownerInfo = tagManager.getTagForMetadataObject(METALAKE, tableObject, ownerTag.name());
    Assertions.assertArrayEquals(new String[] {"team-a"}, ownerInfo.assignment().get().values());
    Assertions.assertArrayEquals(
        new MetadataObject[] {tableObject},
        tagManager.listMetadataObjectsForTag(METALAKE, ownerTag.name(), "team-a"));

    Tag noValueIdempotentTag = tagManager.createTag(METALAKE, "no_value_idempotent", null, null);
    tagManager.associateTagValuesForMetadataObject(
        METALAKE,
        tableObject,
        new TagValue[] {TagValue.noValue(noValueIdempotentTag.name())},
        null);
    Assertions.assertDoesNotThrow(
        () ->
            tagManager.associateTagValuesForMetadataObject(
                METALAKE,
                tableObject,
                new TagValue[] {TagValue.noValue(noValueIdempotentTag.name())},
                null));

    Assertions.assertTrue(
        tagInfos[0].valueConstraint().type() == TagValueConstraint.Type.ALLOWED_VALUES);
    Assertions.assertArrayEquals(
        new String[] {"finance", "risk"}, tagInfos[0].valueConstraint().allowedValues());
  }

  @Test
  public void testV1RemoveValuedTagByName() {
    Tag tag =
        tagManager.createTag(
            METALAKE, "v1_remove_valued", null, null, TagValueConstraint.ofAllowedValues("dev"));
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    tagManager.associateTagValuesForMetadataObject(
        METALAKE, tableObject, new TagValue[] {TagValue.of(tag.name(), "dev")}, null);
    Assertions.assertArrayEquals(
        new MetadataObject[] {tableObject},
        tagManager.listMetadataObjectsForTag(METALAKE, tag.name(), "dev"));

    tagManager.associateTagsForMetadataObject(
        METALAKE, tableObject, null, new String[] {tag.name()});

    Assertions.assertEquals(
        0, tagManager.listTagsInfoForMetadataObject(METALAKE, tableObject).length);
    Assertions.assertEquals(
        0, tagManager.listMetadataObjectsForTag(METALAKE, tag.name(), "dev").length);
  }

  @Test
  public void testRejectNullTagValueToRemove() {
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    NullPointerException exception =
        Assertions.assertThrows(
            NullPointerException.class,
            () ->
                tagManager.associateTagValuesForMetadataObject(
                    METALAKE, tableObject, null, new TagValue[] {null}));
    Assertions.assertEquals("Tag value to remove must not be null", exception.getMessage());
  }

  @Test
  public void testRejectMixedTagAdditionsWithAndWithoutValues() {
    Tag noValueFirstTag = tagManager.createTag(METALAKE, "no_value_first", null, null);
    Tag valuedFirstTag = tagManager.createTag(METALAKE, "valued_first", null, null);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tagManager.associateTagValuesForMetadataObject(
                METALAKE,
                tableObject,
                new TagValue[] {
                  TagValue.noValue(noValueFirstTag.name()),
                  TagValue.of(noValueFirstTag.name(), "finance")
                },
                null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tagManager.associateTagValuesForMetadataObject(
                METALAKE,
                tableObject,
                new TagValue[] {
                  TagValue.of(valuedFirstTag.name(), "finance"),
                  TagValue.noValue(valuedFirstTag.name())
                },
                null));
  }

  @Test
  public void testListMetadataObjectsForTag() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);
    MetadataObject viewObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofView(METALAKE, CATALOG, SCHEMA, VIEW), Entity.EntityType.VIEW);
    MetadataObject functionObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofFunction(METALAKE, CATALOG, SCHEMA, FUNCTION),
            Entity.EntityType.FUNCTION);

    tagManager.associateTagsForMetadataObject(
        METALAKE, catalogObject, new String[] {tag1.name(), tag2.name(), tag3.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, schemaObject, new String[] {tag1.name(), tag2.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, tableObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, columnObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, viewObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, functionObject, new String[] {tag1.name()}, null);

    MetadataObject[] objects = tagManager.listMetadataObjectsForTag(METALAKE, tag1.name());
    Assertions.assertEquals(6, objects.length);
    Assertions.assertEquals(
        ImmutableSet.of(
            catalogObject, schemaObject, tableObject, columnObject, viewObject, functionObject),
        ImmutableSet.copyOf(objects));

    MetadataObject[] objects1 = tagManager.listMetadataObjectsForTag(METALAKE, tag2.name());
    Assertions.assertEquals(2, objects1.length);
    Assertions.assertEquals(
        ImmutableSet.of(catalogObject, schemaObject), ImmutableSet.copyOf(objects1));

    MetadataObject[] objects2 = tagManager.listMetadataObjectsForTag(METALAKE, tag3.name());
    Assertions.assertEquals(1, objects2.length);
    Assertions.assertEquals(ImmutableSet.of(catalogObject), ImmutableSet.copyOf(objects2));

    // List metadata objects for non-existent tag
    Throwable e =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.listMetadataObjectsForTag(METALAKE, "non_existent_tag"));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Tag with name non_existent_tag under metalake " + METALAKE + " does not exist"));
  }

  @Test
  public void testListTagsForMetadataObject() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);
    MetadataObject viewObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofView(METALAKE, CATALOG, SCHEMA, VIEW), Entity.EntityType.VIEW);
    MetadataObject functionObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofFunction(METALAKE, CATALOG, SCHEMA, FUNCTION),
            Entity.EntityType.FUNCTION);

    tagManager.associateTagsForMetadataObject(
        METALAKE, catalogObject, new String[] {tag1.name(), tag2.name(), tag3.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, schemaObject, new String[] {tag1.name(), tag2.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, tableObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, columnObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, viewObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, functionObject, new String[] {tag1.name()}, null);

    String[] tags = tagManager.listTagsForMetadataObject(METALAKE, catalogObject);
    Assertions.assertEquals(3, tags.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2", "tag3"), ImmutableSet.copyOf(tags));

    Tag[] tagsInfo = tagManager.listTagsInfoForMetadataObject(METALAKE, catalogObject);
    Assertions.assertEquals(3, tagsInfo.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2", "tag3"), tagNames(tagsInfo));
    Arrays.stream(tagsInfo).forEach(tag -> assertAssignmentValues(tag, new String[0]));

    String[] tags1 = tagManager.listTagsForMetadataObject(METALAKE, schemaObject);
    Assertions.assertEquals(2, tags1.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2"), ImmutableSet.copyOf(tags1));

    Tag[] tagsInfo1 = tagManager.listTagsInfoForMetadataObject(METALAKE, schemaObject);
    Assertions.assertEquals(2, tagsInfo1.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2"), tagNames(tagsInfo1));
    Arrays.stream(tagsInfo1).forEach(tag -> assertAssignmentValues(tag, new String[0]));

    String[] tags2 = tagManager.listTagsForMetadataObject(METALAKE, tableObject);
    Assertions.assertEquals(1, tags2.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags2));

    Tag[] tagsInfo2 = tagManager.listTagsInfoForMetadataObject(METALAKE, tableObject);
    Assertions.assertEquals(1, tagsInfo2.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), tagNames(tagsInfo2));
    Arrays.stream(tagsInfo2).forEach(tag -> assertAssignmentValues(tag, new String[0]));

    String[] tags3 = tagManager.listTagsForMetadataObject(METALAKE, columnObject);
    Assertions.assertEquals(1, tags3.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags3));

    Tag[] tagsInfo3 = tagManager.listTagsInfoForMetadataObject(METALAKE, columnObject);
    Assertions.assertEquals(1, tagsInfo3.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), tagNames(tagsInfo3));
    Arrays.stream(tagsInfo3).forEach(tag -> assertAssignmentValues(tag, new String[0]));

    String[] tags4 = tagManager.listTagsForMetadataObject(METALAKE, viewObject);
    Assertions.assertEquals(1, tags4.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags4));

    Tag[] tagsInfo4 = tagManager.listTagsInfoForMetadataObject(METALAKE, viewObject);
    Assertions.assertEquals(1, tagsInfo4.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), tagNames(tagsInfo4));
    Arrays.stream(tagsInfo4).forEach(tag -> assertAssignmentValues(tag, new String[0]));

    String[] tags5 = tagManager.listTagsForMetadataObject(METALAKE, functionObject);
    Assertions.assertEquals(1, tags5.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags5));

    Tag[] tagsInfo5 = tagManager.listTagsInfoForMetadataObject(METALAKE, functionObject);
    Assertions.assertEquals(1, tagsInfo5.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), tagNames(tagsInfo5));
    Arrays.stream(tagsInfo5).forEach(tag -> assertAssignmentValues(tag, new String[0]));

    // List tags for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e =
        Assertions.assertThrows(
            NotFoundException.class,
            () -> tagManager.listTagsForMetadataObject(METALAKE, nonExistentObject));
    Assertions.assertTrue(
        e.getMessage().contains("Failed to list tags for metadata object " + nonExistentObject));
  }

  @Test
  public void testGetTagForMetadataObject() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);
    MetadataObject viewObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofView(METALAKE, CATALOG, SCHEMA, VIEW), Entity.EntityType.VIEW);
    MetadataObject functionObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofFunction(METALAKE, CATALOG, SCHEMA, FUNCTION),
            Entity.EntityType.FUNCTION);

    tagManager.associateTagsForMetadataObject(
        METALAKE, catalogObject, new String[] {tag1.name(), tag2.name(), tag3.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, schemaObject, new String[] {tag1.name(), tag2.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, tableObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, columnObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, viewObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, functionObject, new String[] {tag1.name()}, null);

    Tag result = tagManager.getTagForMetadataObject(METALAKE, catalogObject, tag1.name());
    assertTagMetadataEquals(tag1, result);
    assertAssignmentValues(result, new String[0]);

    Tag result1 = tagManager.getTagForMetadataObject(METALAKE, schemaObject, tag1.name());
    assertTagMetadataEquals(tag1, result1);
    assertAssignmentValues(result1, new String[0]);

    Tag result2 = tagManager.getTagForMetadataObject(METALAKE, schemaObject, tag2.name());
    assertTagMetadataEquals(tag2, result2);
    assertAssignmentValues(result2, new String[0]);

    Tag result3 = tagManager.getTagForMetadataObject(METALAKE, catalogObject, tag3.name());
    assertTagMetadataEquals(tag3, result3);
    assertAssignmentValues(result3, new String[0]);

    Tag result4 = tagManager.getTagForMetadataObject(METALAKE, tableObject, tag1.name());
    assertTagMetadataEquals(tag1, result4);
    assertAssignmentValues(result4, new String[0]);

    Tag result5 = tagManager.getTagForMetadataObject(METALAKE, viewObject, tag1.name());
    assertTagMetadataEquals(tag1, result5);
    assertAssignmentValues(result5, new String[0]);

    Tag result6 = tagManager.getTagForMetadataObject(METALAKE, functionObject, tag1.name());
    assertTagMetadataEquals(tag1, result6);
    assertAssignmentValues(result6, new String[0]);

    // Test get non-existent tag for metadata object
    Throwable e =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, catalogObject, "non_existent_tag"));
    Assertions.assertTrue(e.getMessage().contains("Tag non_existent_tag does not exist"));

    Throwable e1 =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, schemaObject, tag3.name()));
    Assertions.assertTrue(e1.getMessage().contains("Tag tag3 does not exist"));

    Throwable e2 =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, tableObject, tag2.name()));
    Assertions.assertTrue(e2.getMessage().contains("Tag tag2 does not exist"));

    // Test get tag for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e3 =
        Assertions.assertThrows(
            NotFoundException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, nonExistentObject, tag1.name()));
    Assertions.assertTrue(
        e3.getMessage().contains("Failed to get tag for metadata object " + nonExistentObject));
  }

  @Test
  public void testSemanticModelIsSupportedForTags() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);

    MetadataObject semanticModelObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSemanticModel(METALAKE, CATALOG, SCHEMA, SEMANTIC_MODEL),
            Entity.EntityType.SEMANTIC_MODEL);
    Assertions.assertEquals(MetadataObject.Type.SEMANTIC_MODEL, semanticModelObject.type());
    Assertions.assertEquals(
        CATALOG + "." + SCHEMA + "." + SEMANTIC_MODEL, semanticModelObject.fullName());

    // A Semantic Model is an accepted tag target, so an absent one must fail existence validation
    // rather than be rejected as an unsupported metadata object type.
    when(semanticModelDispatcher.semanticModelExists(any())).thenReturn(false);

    Throwable e =
        Assertions.assertThrows(
            NoSuchMetadataObjectException.class,
            () ->
                tagManager.associateTagsForMetadataObject(
                    METALAKE, semanticModelObject, new String[] {tag1.name()}, null));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Metadata object "
                    + semanticModelObject.fullName()
                    + " type SEMANTIC_MODEL doesn't exist"),
        e.getMessage());

    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> tagManager.listTagsForMetadataObject(METALAKE, semanticModelObject));
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> tagManager.getTagForMetadataObject(METALAKE, semanticModelObject, tag1.name()));
  }

  /** The old generic reserved prefix is available for ordinary tags. */
  @Test
  public void testLegacyReservedTagsRemainAccessible() throws Exception {
    MetadataObject object = MetadataObjects.parse(CATALOG, MetadataObject.Type.CATALOG);
    for (String name : new String[] {"reserved_legacy", "Reserved_legacy"}) {
      TagEntity legacy =
          TagEntity.builder()
              .withId(idGenerator.nextId())
              .withName(name)
              .withNamespace(Namespace.of(METALAKE))
              .withAuditInfo(
                  AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
              .build();
      entityStore.put(legacy, false);
      Assertions.assertTrue(Arrays.asList(tagManager.listTags(METALAKE)).contains(name));
      Assertions.assertEquals(name, tagManager.getTag(METALAKE, name).name());
      Assertions.assertEquals(
          "updated",
          tagManager.alterTag(METALAKE, name, TagChange.updateComment("updated")).comment());
      tagManager.associateTagsForMetadataObject(METALAKE, object, new String[] {name}, null);
      Assertions.assertEquals(
          name, tagManager.getTagForMetadataObject(METALAKE, object, name).name());
      Assertions.assertArrayEquals(
          new MetadataObject[] {object}, tagManager.listMetadataObjectsForTag(METALAKE, name));
      Assertions.assertEquals(0, tagManager.listPolicyAssociationsForTag(METALAKE, name).length);
      tagManager.associateTagsForMetadataObject(METALAKE, object, null, new String[] {name});
      Assertions.assertTrue(tagManager.deleteTag(METALAKE, name));

      entityStore.put(
          TagEntity.builder()
              .withId(idGenerator.nextId())
              .withName(name)
              .withNamespace(legacy.namespace())
              .withAuditInfo(legacy.auditInfo())
              .build(),
          false);
      String newName = "legacy_" + name;
      tagManager.alterTag(METALAKE, name, TagChange.rename(newName));
      Assertions.assertEquals(newName, tagManager.getTag(METALAKE, newName).name());
      tagManager.alterTag(METALAKE, newName, TagChange.rename(name));
      Assertions.assertEquals(name, tagManager.getTag(METALAKE, name).name());
      Assertions.assertTrue(tagManager.deleteTag(METALAKE, name));
      Assertions.assertEquals(name, tagManager.createTag(METALAKE, name, null, null).name());
      Assertions.assertTrue(tagManager.deleteTag(METALAKE, name));
    }
  }

  @Test
  public void testPolicyAssociationsForTag() throws IOException {
    String tagName = "policy_tag";
    String policyName = "policy_for_tag";
    Assertions.assertThrows(
        NoSuchTagException.class, () -> tagManager.listPolicyAssociationsForTag(METALAKE, tagName));
    tagManager.createTag(
        METALAKE, tagName, null, null, TagValueConstraint.ofAllowedValues("finance", "risk"));
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(idGenerator.nextId())
            .withName(policyName)
            .withNamespace(Namespace.of(METALAKE))
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(true)
            .withContent(
                PolicyContents.custom(
                    ImmutableMap.of("rule", "value"),
                    ImmutableSet.of(MetadataObject.Type.TABLE),
                    null))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    entityStore.put(policy, false);

    try {
      tagManager.addPolicyForTag(METALAKE, tagName, policyName, AllValuesSelector.get());
      RelationalEntity<?>[] associations =
          tagManager.listPolicyAssociationsForTag(METALAKE, tagName);
      Assertions.assertEquals(1, associations.length);
      Assertions.assertEquals(policyName, associations[0].targetEntity().name());
      Assertions.assertEquals(tagName, associations[0].source().name());
      Assertions.assertSame(
          AllValuesSelector.get(),
          PolicyAssociationSelectorSerde.deserialize(
              associations[0].relationValue().orElseThrow()));

      Assertions.assertThrows(
          PolicyAlreadyAssociatedException.class,
          () -> tagManager.addPolicyForTag(METALAKE, tagName, policyName, AllValuesSelector.get()));
      Assertions.assertThrows(
          PolicyAlreadyAssociatedException.class,
          () ->
              tagManager.addPolicyForTag(
                  METALAKE, tagName, policyName, TagValueSelector.of("finance")));

      tagManager.removePolicyFromTag(METALAKE, tagName, policyName);
      Assertions.assertEquals(0, tagManager.listPolicyAssociationsForTag(METALAKE, tagName).length);

      tagManager.addPolicyForTag(METALAKE, tagName, policyName, TagValueSelector.of("engineering"));
      associations = tagManager.listPolicyAssociationsForTag(METALAKE, tagName);
      Assertions.assertEquals(
          TagValueSelector.of("engineering"),
          PolicyAssociationSelectorSerde.deserialize(
              associations[0].relationValue().orElseThrow()));
      tagManager.removePolicyFromTag(METALAKE, tagName, policyName);
    } finally {
      entityStore.delete(
          NameIdentifierUtil.ofPolicy(METALAKE, policyName), Entity.EntityType.POLICY);
    }
  }

  @Test
  public void testTagOperationsDoNotWaitForMetalakeTreeLock() throws Exception {
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    String policyName = "lock_free_policy";
    entityStore.put(
        PolicyEntity.builder()
            .withId(idGenerator.nextId())
            .withName(policyName)
            .withNamespace(Namespace.of(METALAKE))
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(true)
            .withContent(
                PolicyContents.custom(
                    ImmutableMap.of("rule", "value"),
                    ImmutableSet.of(MetadataObject.Type.TABLE),
                    null))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build(),
        false);

    // A metalake WRITE lock covers every tag, policy and metadata object below it. Tag operations
    // are fenced by the entity store, so none of them may wait for it.
    try (TreeLockTestSupport.HeldLock metalakeWriter =
        TreeLockTestSupport.HeldLock.acquire(NameIdentifier.of(METALAKE), LockType.WRITE)) {
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter, () -> tagManager.createTag(METALAKE, "lock_free_tag", null, null));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter, () -> tagManager.getTag(METALAKE, "lock_free_tag"));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter, () -> tagManager.listTagsInfo(METALAKE));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter,
          () -> tagManager.alterTag(METALAKE, "lock_free_tag", TagChange.updateComment("changed")));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter,
          () ->
              tagManager.addPolicyForTag(
                  METALAKE, "lock_free_tag", policyName, AllValuesSelector.get()));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter, () -> tagManager.listPolicyAssociationsForTag(METALAKE, "lock_free_tag"));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter,
          () -> tagManager.removePolicyFromTag(METALAKE, "lock_free_tag", policyName));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter,
          () ->
              tagManager.associateTagsForMetadataObject(
                  METALAKE, tableObject, new String[] {"lock_free_tag"}, null));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter, () -> tagManager.listTagsForMetadataObject(METALAKE, tableObject));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter,
          () -> tagManager.getTagForMetadataObject(METALAKE, tableObject, "lock_free_tag"));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter, () -> tagManager.listMetadataObjectsForTag(METALAKE, "lock_free_tag"));
      TreeLockTestSupport.assertRunsConcurrentlyWith(
          metalakeWriter, () -> tagManager.deleteTag(METALAKE, "lock_free_tag"));
    } finally {
      entityStore.delete(
          NameIdentifierUtil.ofPolicy(METALAKE, policyName), Entity.EntityType.POLICY);
    }
  }

  @Test
  public void testConcurrentCreateTagHasOneWinner() throws Exception {
    List<Object> outcomes =
        RaceTestUtils.runTogether(8, () -> tagManager.createTag(METALAKE, "race_tag", null, null));

    Assertions.assertEquals(1, outcomes.stream().filter(o -> o instanceof Tag).count());
    outcomes.stream()
        .filter(o -> !(o instanceof Tag))
        .forEach(o -> Assertions.assertInstanceOf(TagAlreadyExistsException.class, o));
    Assertions.assertEquals("race_tag", tagManager.getTag(METALAKE, "race_tag").name());
  }

  @Test
  public void testConcurrentDeleteTagAndAssociateLeavesNoAssignment() throws Exception {
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);

    for (int i = 0; i < 10; i++) {
      String tagName = "race_delete_tag_" + i;
      tagManager.createTag(METALAKE, tagName, null, null);

      List<Object> outcomes =
          RaceTestUtils.runTogether(
              Arrays.asList(
                  () -> tagManager.deleteTag(METALAKE, tagName),
                  () ->
                      tagManager.associateTagsForMetadataObject(
                          METALAKE, tableObject, new String[] {tagName}, null)));

      Assertions.assertEquals(Boolean.TRUE, outcomes.get(0));
      Object associate = outcomes.get(1);
      // A tag dropped while the association runs is ignored like a tag that never existed, so the
      // association always succeeds, whichever change commits first.
      Assertions.assertInstanceOf(String[].class, associate);
      Assertions.assertThrows(NoSuchTagException.class, () -> tagManager.getTag(METALAKE, tagName));
      Assertions.assertFalse(
          Arrays.asList(tagManager.listTagsForMetadataObject(METALAKE, tableObject))
              .contains(tagName));
    }
  }

  private static Set<String> tagNames(Tag[] tags) {
    return Arrays.stream(tags).map(Tag::name).collect(Collectors.toSet());
  }

  private static void assertTagMetadataEquals(Tag expected, Tag actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.properties(), actual.properties());
    Assertions.assertEquals(expected.valueConstraint().type(), actual.valueConstraint().type());
    Assertions.assertArrayEquals(
        expected.valueConstraint().allowedValues(), actual.valueConstraint().allowedValues());
  }

  private static void assertAssignmentValues(Tag tag, String[] expectedValues) {
    Assertions.assertTrue(tag.assignment().isPresent());
    Assertions.assertArrayEquals(expectedValues, tag.assignment().get().values());
  }

  private static NameIdentifier putTableWithColumn(String tableName, String columnName)
      throws IOException {
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, SCHEMA, tableName);
    entityStore.put(
        TableEntity.builder()
            .withId(idGenerator.nextId())
            .withName(tableName)
            .withNamespace(ident.namespace())
            .withColumns(
                Lists.newArrayList(
                    ColumnEntity.builder()
                        .withId(idGenerator.nextId())
                        .withName(columnName)
                        .withPosition(0)
                        .withDataType(Types.IntegerType.get())
                        .withNullable(true)
                        .withAutoIncrement(false)
                        .withAuditInfo(audit)
                        .build()))
            .withAuditInfo(audit)
            .build(),
        false);
    return ident;
  }

  private static MetadataObject tagTarget(Entity.EntityType type, boolean upperCase) {
    String schema = upperCase ? SCHEMA.toUpperCase(Locale.ROOT) : SCHEMA;
    String table = upperCase ? TABLE.toUpperCase(Locale.ROOT) : TABLE;
    NameIdentifier ident;
    switch (type) {
      case SCHEMA:
        ident = NameIdentifier.of(METALAKE, CATALOG, schema);
        break;
      case TABLE:
        ident = NameIdentifier.of(METALAKE, CATALOG, schema, table);
        break;
      case COLUMN:
        String column = upperCase ? COLUMN.toUpperCase(Locale.ROOT) : COLUMN;
        ident = NameIdentifier.of(METALAKE, CATALOG, schema, table, column);
        break;
      case VIEW:
      case FUNCTION:
        String name = type == Entity.EntityType.VIEW ? VIEW : FUNCTION;
        name = upperCase ? name.toUpperCase(Locale.ROOT) : name;
        ident = NameIdentifier.of(METALAKE, CATALOG, schema, name);
        break;
      default:
        throw new IllegalArgumentException("Unsupported test target: " + type);
    }
    return NameIdentifierUtil.toMetadataObject(ident, type);
  }

  private static void assertTagRoundTrip(
      MetadataObject request, MetadataObject alias, MetadataObject storedObject) {
    Tag tag = tagManager.createTag(METALAKE, "normalization_tag", null, null);
    String[] names = {tag.name()};
    tagManager.associateTagsForMetadataObject(METALAKE, alias, names, null);
    Assertions.assertArrayEquals(names, tagManager.listTagsForMetadataObject(METALAKE, request));
    Assertions.assertEquals(
        tag.name(), tagManager.listTagsInfoForMetadataObject(METALAKE, request)[0].name());
    Assertions.assertEquals(
        tag.name(), tagManager.getTagForMetadataObject(METALAKE, request, tag.name()).name());
    Assertions.assertArrayEquals(
        new MetadataObject[] {storedObject},
        tagManager.listMetadataObjectsForTag(METALAKE, tag.name()));

    Assertions.assertEquals(
        0, tagManager.associateTagsForMetadataObject(METALAKE, request, null, names).length);
    Assertions.assertArrayEquals(
        names, tagManager.associateTagsForMetadataObject(METALAKE, request, names, null));
    tagManager.associateTagsForMetadataObject(METALAKE, alias, null, names);
    TagValue[] values = {TagValue.of(tag.name(), "MixedCaseValue")};
    Assertions.assertArrayEquals(
        names, tagManager.associateTagValuesForMetadataObject(METALAKE, request, values, null));
    Assertions.assertArrayEquals(
        new String[] {"MixedCaseValue"},
        tagManager
            .getTagForMetadataObject(METALAKE, alias, tag.name())
            .assignment()
            .get()
            .values());
    Assertions.assertEquals(
        0, tagManager.associateTagValuesForMetadataObject(METALAKE, request, null, values).length);
    Assertions.assertEquals(0, tagManager.listTagsInfoForMetadataObject(METALAKE, alias).length);
  }
}
