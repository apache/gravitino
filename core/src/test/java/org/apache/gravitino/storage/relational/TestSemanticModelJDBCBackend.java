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
package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dimension;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.storage.relational.service.POStorageReadRouting;
import org.apache.gravitino.storage.relational.service.SemanticModelMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;

/** Tests Semantic Model persistence and parent lifecycle behavior through {@link JDBCBackend}. */
public class TestSemanticModelJDBCBackend extends TestJDBCBackend {

  @TestTemplate
  public void testCreateAndLoadRoundTrip() throws IOException {
    Namespace namespace = createParents("round_trip");
    SemanticModelEntity absent =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "absent_optional_model",
            false,
            ImmutableMap.of("domain", "sales"));
    SemanticModelEntity empty =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "empty_optional_model",
            true,
            ImmutableMap.of());

    backend.insert(absent, false);
    backend.insert(empty, false);

    SemanticModelEntity loadedAbsent =
        backend.get(absent.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL);
    SemanticModelEntity loadedEmpty =
        backend.get(empty.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL);

    assertEquals(absent, loadedAbsent);
    assertEquals(empty, loadedEmpty);
    assertNull(loadedAbsent.definition().relationships());
    assertNull(loadedAbsent.definition().metrics());
    assertEquals(0, loadedEmpty.definition().relationships().length);
    assertEquals(0, loadedEmpty.definition().metrics().length);
    assertTrue(loadedEmpty.properties().isEmpty());
    assertEquals(
        new BigDecimal("1.50"),
        loadedAbsent.definition().aiContext().object().additionalProperties().get("threshold"));
    assertNull(loadedAbsent.definition().aiContext().object().examples());
    assertEquals(0, loadedAbsent.definition().aiContext().object().synonyms().length);
  }

  @TestTemplate
  public void testDuplicateAndMissingEntities() throws IOException {
    Namespace namespace = createParents("duplicate");
    SemanticModelEntity original =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "sales_model",
            false,
            ImmutableMap.of("owner", "analytics"));
    backend.insert(original, false);

    SemanticModelEntity duplicate =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            original.name(),
            true,
            ImmutableMap.of());
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(duplicate, false));
    assertEquals(
        original, backend.get(original.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));

    NameIdentifier missingModel = NameIdentifier.of(namespace, "missing_model");
    assertThrows(
        NoSuchEntityException.class,
        () -> backend.get(missingModel, Entity.EntityType.SEMANTIC_MODEL));

    String suffix = Long.toUnsignedString(RandomIdGenerator.INSTANCE.nextId());
    String metalakeName = "missing_parent_metalake_" + suffix;
    String catalogName = "missing_parent_catalog_" + suffix;
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    Namespace missingParentNamespace =
        NamespaceUtil.ofSemanticModel(metalakeName, catalogName, "missing_schema");
    SemanticModelEntity missingParent =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            missingParentNamespace,
            "orphan_model",
            false,
            ImmutableMap.of());

    assertThrows(NoSuchEntityException.class, () -> backend.insert(missingParent, false));
    assertEquals(0, countRows(SemanticModelMetaMapper.TABLE_NAME, missingParent.id()));
    assertEquals(0, countRows(SemanticModelVersionInfoMapper.TABLE_NAME, missingParent.id()));
  }

  @TestTemplate
  public void testCreateRollsBackIdentityWhenSnapshotInsertFails() throws IOException {
    Namespace namespace = createParents("transaction");
    SemanticModelEntity semanticModel =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "transaction_model",
            false,
            ImmutableMap.of("domain", "finance"));
    SemanticModelPO po =
        SemanticModelPO.initializeSemanticModelPO(semanticModel, SemanticModelPO.builder());
    SessionUtils.doWithCommit(
        SemanticModelVersionInfoMapper.class,
        mapper -> mapper.insertSemanticModelVersionInfo(po.getSemanticModelVersionInfoPO()));

    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(semanticModel, false));
    assertEquals(0, countRows(SemanticModelMetaMapper.TABLE_NAME, semanticModel.id()));
    assertEquals(1, countRows(SemanticModelVersionInfoMapper.TABLE_NAME, semanticModel.id()));
    assertEquals(0, countEntityChanges());
  }

  @TestTemplate
  public void testOverwriteAdvancesVersionAndRetainsPreviousSnapshot() throws IOException {
    Namespace namespace = createParents("overwrite_version");
    SemanticModelEntity original =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "versioned_model",
            false,
            ImmutableMap.of("revision", "one"));
    SemanticModelEntity replacement =
        semanticModel(
            original.id(), namespace, original.name(), true, ImmutableMap.of("revision", "two"));

    backend.insert(original, false);
    backend.insert(replacement, true);

    assertEquals(
        replacement, backend.get(original.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    SemanticModelPO persisted = getSemanticModelPO(original.id());
    assertEquals(2, persisted.getCurrentVersion());
    assertEquals(2, persisted.getLastVersion());
    assertEquals(List.of(1, 2), activeSnapshotVersions(original.id()));
    assertEquals(0, countEntityChanges());
  }

  @TestTemplate
  public void testNaturalKeyOverwriteUsesPersistedSemanticModelId() throws IOException {
    Namespace namespace = createParents("natural_key_overwrite");
    SemanticModelEntity original =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "natural_key_model",
            false,
            ImmutableMap.of("revision", "one"));
    SemanticModelEntity replacement =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            original.name(),
            true,
            ImmutableMap.of("revision", "two"));
    SemanticModelEntity expected =
        semanticModel(
            original.id(), namespace, original.name(), true, ImmutableMap.of("revision", "two"));

    backend.insert(original, false);
    backend.insert(replacement, true);

    assertEquals(
        expected, backend.get(original.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    SemanticModelPO persisted = getSemanticModelPO(original.id());
    assertEquals(2, persisted.getCurrentVersion());
    assertEquals(2, persisted.getLastVersion());
    assertEquals(List.of(1, 2), activeSnapshotVersions(original.id()));
    assertEquals(0, countRows(SemanticModelMetaMapper.TABLE_NAME, replacement.id()));
    assertEquals(0, countRows(SemanticModelVersionInfoMapper.TABLE_NAME, replacement.id()));
  }

  @TestTemplate
  public void testOverwriteInsertCreatesMissingSemanticModel() throws IOException {
    Namespace namespace = createParents("overwrite_insert");
    SemanticModelEntity imported =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "imported_model",
            false,
            ImmutableMap.of("source", "external"));

    backend.insert(imported, true);

    assertEquals(
        imported, backend.get(imported.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    assertEquals(1, countRows(SemanticModelMetaMapper.TABLE_NAME, imported.id()));
    assertEquals(1, countRows(SemanticModelVersionInfoMapper.TABLE_NAME, imported.id()));
    assertEquals(List.of(1), activeSnapshotVersions(imported.id()));
  }

  @TestTemplate
  public void testSemanticModelReadRoutesAndEntityIdResolver() throws IOException {
    Namespace namespace = createParents("read_routes");
    SemanticModelEntity semanticModel =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "routed_model",
            false,
            ImmutableMap.of("domain", "sales"));
    backend.insert(semanticModel, false);

    SemanticModelPO byParentId = readSemanticModelPO(semanticModel.nameIdentifier(), true);
    SemanticModelPO byFullName = readSemanticModelPO(semanticModel.nameIdentifier(), false);
    assertEquals(semanticModel.id(), byParentId.getSemanticModelId());
    assertEquals(semanticModel.id(), byFullName.getSemanticModelId());

    RelationalEntityStoreIdResolver resolver = new RelationalEntityStoreIdResolver();
    NamespacedEntityId resolved =
        resolver.getEntityIds(semanticModel.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL);
    assertEquals(semanticModel.id().longValue(), resolved.entityId());

    String metalake = namespace.level(0);
    String catalog = namespace.level(1);
    String schema = namespace.level(2);
    List<NameIdentifier> missingParents =
        List.of(
            NameIdentifier.of(
                NamespaceUtil.ofSemanticModel("missing_metalake", catalog, schema),
                semanticModel.name()),
            NameIdentifier.of(
                NamespaceUtil.ofSemanticModel(metalake, "missing_catalog", schema),
                semanticModel.name()),
            NameIdentifier.of(
                NamespaceUtil.ofSemanticModel(metalake, catalog, "missing_schema"),
                semanticModel.name()));
    for (NameIdentifier missing : missingParents) {
      assertThrows(NoSuchEntityException.class, () -> readSemanticModelPO(missing, true));
      assertThrows(NoSuchEntityException.class, () -> readSemanticModelPO(missing, false));
      assertThrows(
          NoSuchEntityException.class,
          () -> resolver.getEntityIds(missing, Entity.EntityType.SEMANTIC_MODEL));
    }

    NameIdentifier missingModel = NameIdentifier.of(namespace, "missing_model");
    assertNull(readSemanticModelPO(missingModel, true));
    assertNull(readSemanticModelPO(missingModel, false));
    assertThrows(
        NoSuchEntityException.class,
        () -> resolver.getEntityIds(missingModel, Entity.EntityType.SEMANTIC_MODEL));
  }

  @TestTemplate
  public void testConcurrentCreatesAndOverwriteRacesRemainAtomic() throws Exception {
    Namespace namespace = createParents("concurrent");
    SemanticModelEntity first =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "concurrent_model",
            false,
            ImmutableMap.of("candidate", "first"));
    SemanticModelEntity second =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            first.name(),
            true,
            ImmutableMap.of("candidate", "second"));

    List<Throwable> createResults = insertConcurrently(first, false, second, false);
    assertEquals(1, createResults.stream().filter(Objects::isNull).count());
    Throwable createFailure =
        createResults.stream().filter(Objects::nonNull).findFirst().orElseThrow();
    assertTrue(createFailure instanceof EntityAlreadyExistsException);
    assertEquals(
        1,
        countRows(SemanticModelMetaMapper.TABLE_NAME, first.id())
            + countRows(SemanticModelMetaMapper.TABLE_NAME, second.id()));
    assertEquals(
        1,
        countRows(SemanticModelVersionInfoMapper.TABLE_NAME, first.id())
            + countRows(SemanticModelVersionInfoMapper.TABLE_NAME, second.id()));

    SemanticModelEntity created =
        backend.get(first.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL);
    SemanticModelEntity overwriteOne =
        semanticModel(
            created.id(), namespace, created.name(), false, ImmutableMap.of("winner", "one"));
    SemanticModelEntity overwriteTwo =
        semanticModel(
            created.id(), namespace, created.name(), true, ImmutableMap.of("winner", "two"));

    List<Throwable> overwriteResults = insertConcurrently(overwriteOne, true, overwriteTwo, true);
    assertTrue(overwriteResults.stream().allMatch(Objects::isNull));

    SemanticModelPO persisted = getSemanticModelPO(created.id());
    assertEquals(3, persisted.getCurrentVersion());
    assertEquals(3, persisted.getLastVersion());
    assertEquals(List.of(1, 2, 3), activeSnapshotVersions(created.id()));
    SemanticModelEntity winner =
        backend.get(created.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL);
    assertTrue(winner.equals(overwriteOne) || winner.equals(overwriteTwo));
    assertEquals(0, countEntityChanges());
  }

  @TestTemplate
  public void testListUpdateDeleteAndGarbageCollection() throws IOException {
    Namespace namespace = createParents("lifecycle");
    SemanticModelEntity original =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "sales_model",
            false,
            ImmutableMap.of("domain", "sales"));
    backend.insert(original, false);

    assertEquals(
        List.of(original), backend.list(namespace, Entity.EntityType.SEMANTIC_MODEL, true));
    assertEquals(
        List.of(original),
        backend.batchGet(
            List.of(original.nameIdentifier(), NameIdentifier.of(namespace, "missing_model")),
            Entity.EntityType.SEMANTIC_MODEL));

    SemanticModelEntity renamed =
        semanticModel(
            original.id(),
            namespace,
            "renamed_sales_model",
            false,
            ImmutableMap.of("domain", "sales_v2"));
    assertEquals(
        renamed,
        backend.update(
            original.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL, ignored -> renamed));
    assertFalse(backend.exists(original.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    assertEquals(renamed, backend.get(renamed.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));

    assertTrue(backend.delete(renamed.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL, false));
    assertFalse(backend.exists(renamed.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    assertTrue(legacyRecordExistsInDB(renamed.id(), Entity.EntityType.SEMANTIC_MODEL));
    assertTrue(allVersionRowsAreSoftDeleted(renamed.id()));

    backend.hardDeleteLegacyData(
        Entity.EntityType.SEMANTIC_MODEL, Instant.now().toEpochMilli() + 1000);
    assertFalse(legacyRecordExistsInDB(renamed.id(), Entity.EntityType.SEMANTIC_MODEL));
    assertEquals(0, countRows("semantic_model_version_info", renamed.id()));
  }

  @TestTemplate
  public void testSchemaNonCascadeAndCascadeLifecycle() throws IOException {
    Namespace namespace = createParents("schema_cascade");
    SemanticModelEntity semanticModel =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "schema_child_model",
            false,
            ImmutableMap.of());
    backend.insert(semanticModel, false);

    NameIdentifier schemaIdentifier =
        NameIdentifier.of(namespace.level(0), namespace.level(1), namespace.level(2));
    assertThrows(
        NonEmptyEntityException.class,
        () -> backend.delete(schemaIdentifier, Entity.EntityType.SCHEMA, false));
    assertTrue(backend.exists(semanticModel.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));

    assertTrue(backend.delete(schemaIdentifier, Entity.EntityType.SCHEMA, true));
    assertFalse(backend.exists(semanticModel.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    assertTrue(legacyRecordExistsInDB(semanticModel.id(), Entity.EntityType.SEMANTIC_MODEL));
    assertTrue(allVersionRowsAreSoftDeleted(semanticModel.id()));
  }

  @TestTemplate
  public void testCatalogAndMetalakeCascadeLifecycle() throws IOException {
    Namespace catalogNamespace = createParents("catalog_cascade");
    SemanticModelEntity catalogChild =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            catalogNamespace,
            "catalog_child_model",
            false,
            ImmutableMap.of());
    backend.insert(catalogChild, false);

    NameIdentifier catalogIdentifier =
        NameIdentifier.of(catalogNamespace.level(0), catalogNamespace.level(1));
    assertTrue(backend.delete(catalogIdentifier, Entity.EntityType.CATALOG, true));
    assertFalse(backend.exists(catalogChild.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    assertTrue(allVersionRowsAreSoftDeleted(catalogChild.id()));

    Namespace metalakeNamespace = createParents("metalake_cascade");
    SemanticModelEntity metalakeChild =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            metalakeNamespace,
            "metalake_child_model",
            false,
            ImmutableMap.of());
    backend.insert(metalakeChild, false);

    assertTrue(
        backend.delete(
            NameIdentifier.of(metalakeNamespace.level(0)), Entity.EntityType.METALAKE, true));
    assertFalse(backend.exists(metalakeChild.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));
    assertTrue(allVersionRowsAreSoftDeleted(metalakeChild.id()));
  }

  private Namespace createParents(String prefix) throws IOException {
    String suffix = Long.toUnsignedString(RandomIdGenerator.INSTANCE.nextId());
    String metalakeName = prefix + "_metalake_" + suffix;
    String catalogName = prefix + "_catalog_" + suffix;
    String schemaName = prefix + "_schema_" + suffix;
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
    return NamespaceUtil.ofSemanticModel(metalakeName, catalogName, schemaName);
  }

  private SemanticModelEntity semanticModel(
      Long id,
      Namespace namespace,
      String name,
      boolean explicitEmpty,
      Map<String, String> properties) {
    AIContextObject context =
        AIContextObject.builder()
            .withInstructions("Use certified sales definitions")
            .withSynonyms(new String[0])
            .withAdditionalProperties(
                Map.of("threshold", new BigDecimal("1.50"), "nested", List.of(new BigInteger("3"))))
            .build();
    Field field =
        Field.builder()
            .withName("ordered_at")
            .withExpression(
                Expression.builder()
                    .withDialects(
                        new DialectExpression[] {
                          DialectExpression.builder()
                              .withDialect("ansi")
                              .withExpression("ordered_at")
                              .build()
                        })
                    .build())
            .withDimension(Dimension.builder().withIsTime(true).build())
            .withDatatype(DataType.DATE_TIME_TZ)
            .build();
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[0])
            .withFields(new Field[] {field})
            .build();
    SemanticModelDefinition.Builder definitionBuilder =
        SemanticModelDefinition.builder()
            .withAIContext(AIContext.of(context))
            .withDatasets(new Dataset[] {dataset});
    if (explicitEmpty) {
      definitionBuilder
          .withRelationships(new Relationship[0])
          .withMetrics(new Metric[0])
          .withCustomExtensions(new CustomExtension[0]);
    }

    return SemanticModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(explicitEmpty ? null : "Governed sales definitions")
        .withDefinition(definitionBuilder.build())
        .withProperties(properties)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private int countRows(String tableName, Long semanticModelId) {
    if (!SemanticModelMetaMapper.TABLE_NAME.equals(tableName)
        && !SemanticModelVersionInfoMapper.TABLE_NAME.equals(tableName)) {
      throw new IllegalArgumentException("Unsupported Semantic Model table: " + tableName);
    }
    String sql = String.format("SELECT count(*) FROM %s WHERE semantic_model_id = ?", tableName);
    return queryCount(sql, semanticModelId);
  }

  private boolean allVersionRowsAreSoftDeleted(Long semanticModelId) {
    int total = countRows(SemanticModelVersionInfoMapper.TABLE_NAME, semanticModelId);
    if (total == 0) {
      return false;
    }
    return total
        == queryCount(
            "SELECT count(*) FROM "
                + SemanticModelVersionInfoMapper.TABLE_NAME
                + " WHERE semantic_model_id = ? AND deleted_at > 0",
            semanticModelId);
  }

  private int queryCount(String sql, Long semanticModelId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, semanticModelId);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        return resultSet.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to count Semantic Model rows", e);
    }
  }

  private SemanticModelPO getSemanticModelPO(Long semanticModelId) {
    SemanticModelPO po =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper -> mapper.selectSemanticModelMetaById(semanticModelId));
    assertNotNull(po);
    return po;
  }

  private SemanticModelPO readSemanticModelPO(NameIdentifier identifier, boolean cacheEnabled) {
    return SessionUtils.getWithoutCommit(
        SemanticModelMetaMapper.class,
        mapper ->
            POStorageReadRouting.getPO(
                mapper,
                identifier,
                SemanticModelMetaService.getInstance().ops(),
                Entity.EntityType.SEMANTIC_MODEL,
                cacheEnabled));
  }

  private List<Integer> activeSnapshotVersions(Long semanticModelId) {
    String sql =
        String.format(
            "SELECT version FROM %s WHERE semantic_model_id = ? AND deleted_at = 0 ORDER BY version",
            SemanticModelVersionInfoMapper.TABLE_NAME);
    List<Integer> versions = new ArrayList<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, semanticModelId);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          versions.add(resultSet.getInt(1));
        }
      }
      return versions;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to list Semantic Model versions", e);
    }
  }

  private int countEntityChanges() {
    return SessionUtils.getWithoutCommit(
        EntityChangeLogMapper.class,
        mapper ->
            Math.toIntExact(
                mapper.selectEntityChanges(0, 100).stream()
                    .filter(
                        record ->
                            Entity.EntityType.SEMANTIC_MODEL.name().equals(record.getEntityType()))
                    .count()));
  }

  private List<Throwable> insertConcurrently(
      SemanticModelEntity first,
      boolean overwriteFirst,
      SemanticModelEntity second,
      boolean overwriteSecond)
      throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Throwable> firstResult =
          executor.submit(() -> insertAfterStart(first, overwriteFirst, start));
      Future<Throwable> secondResult =
          executor.submit(() -> insertAfterStart(second, overwriteSecond, start));
      start.countDown();
      return Arrays.asList(
          firstResult.get(30, TimeUnit.SECONDS), secondResult.get(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  private Throwable insertAfterStart(
      SemanticModelEntity semanticModel, boolean overwrite, CountDownLatch start) {
    try {
      assertTrue(start.await(30, TimeUnit.SECONDS));
      backend.insert(semanticModel, overwrite);
      return null;
    } catch (Throwable throwable) {
      return throwable;
    }
  }
}
