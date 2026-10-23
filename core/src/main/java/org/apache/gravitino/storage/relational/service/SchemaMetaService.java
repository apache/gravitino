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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogWriter;
import org.apache.gravitino.storage.relational.helper.SchemaIds;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
<<<<<<< HEAD
=======
import org.apache.gravitino.storage.relational.mapper.ViewVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.FunctionPO;
>>>>>>> c816482b8 ([#14119] fix(fileset): Preserve metadata on cascade cleanup failure (#14120))
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** The service class for schema metadata. It provides the basic database operations for schema. */
public class SchemaMetaService {
  private static final SchemaMetaService INSTANCE = new SchemaMetaService();
  private BasePOStorageOps<SchemaPO, SchemaMetaMapper> ops;

  public static SchemaMetaService getInstance() {
    return INSTANCE;
  }

  private SchemaMetaService() {
    this.ops =
        new HierarchicalConversionPOStorageOps<>(
            new SchemaPOStorageOps(),
            SchemaMetaService::physicalToLogicalSchemaPO,
            SchemaMetaService::logicalToPhysicalSchemaPO);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName")
  public SchemaIds getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
      String metalakeName, String catalogName, String schemaName) {
    NameIdentifier identifier = NameIdentifier.of(metalakeName, catalogName, schemaName);
    SchemaPO schemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                POStorageReadRouting.getPO(mapper, identifier, ops, Entity.EntityType.SCHEMA));

    if (schemaPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          schemaName);
    }

    return new SchemaIds(schemaPO.getMetalakeId(), schemaPO.getCatalogId(), schemaPO.getSchemaId());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSchemaByIdentifier")
  public SchemaEntity getSchemaByIdentifier(NameIdentifier identifier) {
    SchemaPO schemaPO = getSchemaPOByIdentifier(identifier);
    return POConverters.fromSchemaPO(schemaPO, identifier.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listSchemasByNamespace")
  public List<SchemaEntity> listSchemasByNamespace(Namespace namespace) {
    NamespaceUtil.checkSchema(namespace);

    List<SchemaPO> schemaPOs = listSchemaPOs(namespace);
    return POConverters.fromSchemaPOs(schemaPOs, namespace);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertSchema")
  public void insertSchema(SchemaEntity schemaEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkSchema(schemaEntity.nameIdentifier());
      // SchemaEntity arrives in API/logical form (separator = HierarchicalSchemaUtil
      // .schemaSeparator()). We split here on the logical separator and build ancestor rows in
      // logical form. HierarchicalConversionPOStorageOps.batchInsertPOs applies its write
      // rewriter to translate each PO's name to storage form before SQL execution.
      String logicalSep = HierarchicalSchemaUtil.schemaSeparator();
      String schemaName = schemaEntity.name();
      List<SchemaEntity> rowsToInsert = new ArrayList<>();
      if (schemaName == null || !schemaName.contains(logicalSep)) {
        rowsToInsert.add(schemaEntity);
      } else {
        // Segments of the logical name; e.g. "A:B:C" -> ancestor rows "A", "A:B", then leaf.
        String[] parts = schemaName.split(Pattern.quote(logicalSep), -1);
        for (int nSeg = 1; nSeg < parts.length; nSeg++) {
          String ancestorLogical = String.join(logicalSep, Arrays.copyOf(parts, nSeg));
          SchemaEntity ancestor =
              SchemaEntity.builder()
                  .withId(nextIdForNestedAncestor())
                  .withName(ancestorLogical)
                  .withNamespace(schemaEntity.namespace())
                  .withComment(null)
                  .withProperties(Collections.emptyMap())
                  .withAuditInfo(schemaEntity.auditInfo())
                  .build();
          rowsToInsert.add(ancestor);
        }
        rowsToInsert.add(schemaEntity);
      }

      SessionUtils.doWithCommit(
          SchemaMetaMapper.class,
          mapper -> {
            int n = rowsToInsert.size();
            List<SchemaPO> missingAncestorPOs = new ArrayList<>();
            if (n > 1) {
              SchemaEntity firstAncestor = rowsToInsert.get(0);
              Namespace ancestorNs = firstAncestor.namespace();
              List<String> ancestorNames =
                  rowsToInsert.subList(0, n - 1).stream()
                      .map(SchemaEntity::name)
                      .collect(Collectors.toList());
              Set<String> existingLogicalNames =
                  ops.listPOs(mapper, ancestorNs, ancestorNames).stream()
                      .map(SchemaPO::getSchemaName)
                      .collect(Collectors.toSet());
              for (SchemaEntity row : rowsToInsert.subList(0, n - 1)) {
                if (existingLogicalNames.contains(row.name())) {
                  continue;
                }
                SchemaPO.Builder builder = SchemaPO.builder();
                fillSchemaPOBuilderParentEntityId(builder, row.namespace());
                missingAncestorPOs.add(POConverters.initializeSchemaPOWithVersion(row, builder));
              }
            }
            SchemaEntity leafRow = rowsToInsert.get(n - 1);
            SchemaPO.Builder leafBuilder = SchemaPO.builder();
            fillSchemaPOBuilderParentEntityId(leafBuilder, leafRow.namespace());
            SchemaPO leafPO = POConverters.initializeSchemaPOWithVersion(leafRow, leafBuilder);
            List<SchemaPO> schemaPosToInsert = new ArrayList<>(missingAncestorPOs);
            schemaPosToInsert.add(leafPO);
            ops.batchInsertPOs(mapper, schemaPosToInsert, overwrite);
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.SCHEMA, schemaEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateSchema")
  public <E extends Entity & HasIdentifier> SchemaEntity updateSchema(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    SchemaPO oldSchemaPO = getSchemaPOByIdentifier(identifier);
    SchemaEntity oldSchemaEntity = POConverters.fromSchemaPO(oldSchemaPO, identifier.namespace());
    SchemaEntity newEntity = (SchemaEntity) updater.apply((E) oldSchemaEntity);
    Preconditions.checkArgument(
        Objects.equals(oldSchemaEntity.id(), newEntity.id()),
        "The updated schema entity id: %s should be same with the schema entity id before: %s",
        newEntity.id(),
        oldSchemaEntity.id());

    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String oldFullName =
        NameIdentifierUtil.ofSchema(metalakeName, catalogName, oldSchemaEntity.name()).toString();
    boolean isRenamed = !Objects.equals(oldSchemaEntity.name(), newEntity.name());

    AtomicInteger updateResult = new AtomicInteger(0);
    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              updateResult.set(
                  SessionUtils.getWithoutCommit(
                      SchemaMetaMapper.class,
                      mapper ->
                          ops.updatePO(
                              mapper,
                              POConverters.updateSchemaPOWithVersion(oldSchemaPO, newEntity),
                              oldSchemaPO))),
          () -> {
            if (isRenamed && updateResult.get() > 0) {
              SessionUtils.doWithoutCommit(
                  EntityChangeLogMapper.class,
                  mapper ->
                      mapper.insertEntityChange(
                          metalakeName,
                          Entity.EntityType.SCHEMA.name(),
                          oldFullName,
                          OperateType.ALTER));
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.SCHEMA, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult.get() > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSchema")
  public boolean deleteSchema(NameIdentifier identifier, boolean cascade) {
    deleteSchemaAndGet(identifier, cascade, false);
    return true;
  }

  /**
   * Deletes a schema and returns the exact entity snapshot protected by the delete CAS.
   *
   * @param identifier the schema identifier
   * @param cascade whether to delete child entities
   * @param cascadeFunctions whether a non-cascading delete should remove functions before checking
   *     for remaining children
   * @return the schema snapshot that was deleted
   */
  public SchemaEntity deleteSchemaAndGet(
      NameIdentifier identifier, boolean cascade, boolean cascadeFunctions) {
    NameIdentifierUtil.checkSchema(identifier);

    String schemaName = identifier.name();
    SchemaPO schemaPO = getSchemaPOByIdentifier(identifier);
    SchemaEntity deletedSchema = POConverters.fromSchemaPO(schemaPO, identifier.namespace());
    Long schemaId = schemaPO.getSchemaId();
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaFullName =
        NameIdentifierUtil.ofSchema(metalakeName, catalogName, schemaName).toString();

    if (cascade) {
      // For HierarchicalSchema, deleting `A:B` must also cascade into all descendant schemas
      // such as `A:B:C`, `A:B:C:D`, etc. Collect the descendant schema ids up-front and run a
      // single batch UPDATE per child table so the total SQL cost stays bounded regardless of
      // how many descendants exist.
      List<Long> schemaIds = listSchemaIdsForCascade(schemaPO);
      if (schemaIds.isEmpty()) {
        return false;
      }
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  TableMetaMapper.class,
                  mapper -> mapper.softDeleteTableMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  TableColumnMapper.class,
                  mapper -> mapper.softDeleteColumnsBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetMetaMapper.class,
                  mapper -> mapper.softDeleteFilesetMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetVersionMapper.class,
                  mapper -> mapper.softDeleteFilesetVersionsBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  TopicMetaMapper.class,
                  mapper -> mapper.softDeleteTopicMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionVersionMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class, mapper -> mapper.softDeleteOwnerRelBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper -> mapper.softDeleteObjectRelsBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  TagMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeleteTagMetadataObjectRelsBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeletePolicyMetadataObjectRelsBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper -> mapper.softDeleteModelVersionAliasRelsBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.softDeleteModelVersionMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelMetaMapper.class,
                  mapper -> mapper.softDeleteModelMetasBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  StatisticMetaMapper.class,
                  mapper -> mapper.softDeleteStatisticsBySchemaIds(schemaIds)),
          () ->
              SessionUtils.doWithoutCommit(
                  ViewMetaMapper.class, mapper -> mapper.softDeleteViewMetasBySchemaIds(schemaIds)),
          () -> {
<<<<<<< HEAD
            SessionUtils.doWithoutCommit(
                EntityChangeLogMapper.class,
                mapper ->
                    mapper.insertEntityChange(
                        metalakeName,
                        Entity.EntityType.SCHEMA.name(),
                        schemaFullName,
                        OperateType.DROP));
          });
    } else {
      List<TableEntity> tableEntities =
          TableMetaService.getInstance()
              .listTablesByNamespace(
                  NamespaceUtil.ofTable(
                      identifier.namespace().level(0),
                      identifier.namespace().level(1),
                      schemaName));
      if (!tableEntities.isEmpty()) {
        throw new NonEmptyEntityException(
            "Entity %s has sub-entities, you should remove sub-entities first", identifier);
      }
      List<FilesetEntity> filesetEntities =
          FilesetMetaService.getInstance()
              .listFilesetsByNamespace(
                  NamespaceUtil.ofFileset(
                      identifier.namespace().level(0),
                      identifier.namespace().level(1),
                      schemaName));
      if (!filesetEntities.isEmpty()) {
        throw new NonEmptyEntityException(
            "Entity %s has sub-entities, you should remove sub-entities first", identifier);
      }
      List<ModelEntity> modelEntities =
          ModelMetaService.getInstance()
              .listModelsByNamespace(
                  NamespaceUtil.ofModel(
                      identifier.namespace().level(0),
                      identifier.namespace().level(1),
                      schemaName));
      if (!modelEntities.isEmpty()) {
        throw new NonEmptyEntityException(
            "Entity %s has sub-entities, you should remove sub-entities first", identifier);
      }

      List<TopicEntity> topicEntities =
          TopicMetaService.getInstance()
              .listTopicsByNamespace(
                  NamespaceUtil.ofTopic(
                      identifier.namespace().level(0),
                      identifier.namespace().level(1),
                      schemaName));
      if (!topicEntities.isEmpty()) {
        throw new NonEmptyEntityException(
            "Entity %s has sub-entities, you should remove sub-entities first", identifier);
      }

      List<Long> singleSchemaId = Collections.singletonList(schemaId);
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasBySchemaIds(singleSchemaId)),
=======
            // Delete the schema first and check that it was empty afterwards. The order matters:
            // the delete locks the schema row, and every child write locks that same row first, so
            // a table or view being created either lands before this delete and shows up in the
            // check, or it waits for this transaction. Checking first would leave a gap for a child
            // to appear in between. A non-empty result throws, which rolls the delete back.
            lockCatalogForSchemaDelete(identifier, schemaPO);
            deleteSchemaWithVersion(identifier, schemaPO);
            if (cascadeFunctions) {
              // The schema row lock excludes new children. Delete functions in this transaction
              // so a remaining fileset or a later cleanup failure restores them with the schema.
              deleteSchemaFunctions(identifier, schemaId);
            }
            checkSchemaIsEmpty(identifier, schemaPO);
          },
>>>>>>> c816482b8 ([#14119] fix(fileset): Preserve metadata on cascade cleanup failure (#14120))
          () ->
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class,
                  mapper ->
                      mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                          schemaId, MetadataObject.Type.SCHEMA.name())),
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper ->
                      mapper.softDeleteObjectRelsByMetadataObject(
                          schemaId, MetadataObject.Type.SCHEMA.name())),
          () ->
              SessionUtils.doWithoutCommit(
                  TagMetadataObjectRelMapper.class,
                  mapper ->
                      mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                          schemaId, MetadataObject.Type.SCHEMA.name())),
          () ->
              SessionUtils.doWithoutCommit(
                  StatisticMetaMapper.class,
                  mapper -> mapper.softDeleteStatisticsByEntityId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyMetadataObjectRelMapper.class,
                  mapper ->
                      mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                          schemaId, MetadataObject.Type.SCHEMA.name())),
          () -> {
            SessionUtils.doWithoutCommit(
                EntityChangeLogMapper.class,
                mapper ->
                    mapper.insertEntityChange(
                        metalakeName,
                        Entity.EntityType.SCHEMA.name(),
                        schemaFullName,
                        OperateType.DROP));
          });
    }
    return deletedSchema;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSchemaMetasByLegacyTimeline")
  public int deleteSchemaMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        SchemaMetaMapper.class,
        mapper -> {
          return mapper.deleteSchemaMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }

  private SchemaPO getSchemaPOByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkSchema(identifier);
    SchemaPO schemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                POStorageReadRouting.getPO(mapper, identifier, ops, Entity.EntityType.SCHEMA));
    if (schemaPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          identifier.name());
    }
    return schemaPO;
  }

  private List<SchemaPO> listSchemaPOs(Namespace namespace) {
    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class,
        mapper -> POStorageReadRouting.listPOs(mapper, namespace, ops, Entity.EntityType.SCHEMA));
  }

  /**
   * Collects the schema ids that participate in a cascade delete: the target schema itself plus
   * every HierarchicalSchema descendant. The {@link SchemaPO} arrives in logical form (e.g. {@code
   * A:B}); {@link HierarchicalConversionPOStorageOps} translates to storage form before running the
   * SQL prefix match, so this method only deals in logical names.
   */
<<<<<<< HEAD
  private List<Long> listSchemaIdsForCascade(SchemaPO schemaPO) {
=======
  private void lockCatalogForSchemaCreate(
      CatalogPO observedCatalogPO, boolean createsImplicitAncestors) {
    OccWriteSupport.lockParentForChildWrite(
        observedCatalogPO.getCatalogName(),
        Entity.EntityType.CATALOG,
        () ->
            SessionUtils.getWithoutCommit(
                CatalogMetaMapper.class,
                mapper ->
                    createsImplicitAncestors
                        ? mapper.selectCatalogMetaByIdForUpdate(observedCatalogPO.getCatalogId())
                        : mapper.selectCatalogMetaByIdForShare(observedCatalogPO.getCatalogId())),
        null,
        current ->
            Objects.equals(current.getCatalogName(), observedCatalogPO.getCatalogName())
                && Objects.equals(current.getMetalakeId(), observedCatalogPO.getMetalakeId()));
  }

  /**
   * Holds the parent catalog row while a schema is dropped. The lock is exclusive here, because a
   * drop removes descendants and must not run next to another drop or create under the same
   * catalog. Taking the catalog before any schema row also gives every drop the same lock order, so
   * two overlapping cascades cannot deadlock.
   */
  private void lockCatalogForSchemaDelete(NameIdentifier identifier, SchemaPO observedSchemaPO) {
    String catalogName = identifier.namespace().level(1);
    OccWriteSupport.lockParentForChildWrite(
        catalogName,
        Entity.EntityType.CATALOG,
        () ->
            SessionUtils.getWithoutCommit(
                CatalogMetaMapper.class,
                mapper -> mapper.selectCatalogMetaByIdForUpdate(observedSchemaPO.getCatalogId())),
        null,
        current ->
            Objects.equals(current.getCatalogName(), catalogName)
                && Objects.equals(current.getMetalakeId(), observedSchemaPO.getMetalakeId()));
  }

  /**
   * Runs schema-scoped writes while holding a shared lock on their parent schema.
   *
   * <p>This method owns the transaction boundary on purpose. If callers locked the schema in one
   * transaction and wrote the child in another, the lock would be released too early and a schema
   * deletion could slip between those two steps. Keeping the lock and every supplied operation in
   * the same transaction makes that mistake impossible for callers of this entry point.
   */
  void doWithSchemaWriteLock(
      NameIdentifier entityIdentifier,
      Long observedSchemaId,
      Long observedCatalogId,
      Long observedMetalakeId,
      Runnable... entityWriteOperations) {
    Runnable[] transactionOperations = new Runnable[entityWriteOperations.length + 1];
    transactionOperations[0] =
        () ->
            lockSchemaForEntityWrite(
                entityIdentifier, observedSchemaId, observedCatalogId, observedMetalakeId);
    System.arraycopy(
        entityWriteOperations, 0, transactionOperations, 1, entityWriteOperations.length);
    SessionUtils.doMultipleWithCommit(transactionOperations);
  }

  /**
   * Takes a shared lock on the parent catalog row before a cross-schema child move.
   *
   * <p>A cascade schema delete holds an exclusive catalog lock before any schema lock. Taking the
   * same shared catalog lock here first ensures that a cross-schema child move blocks the cascade
   * delete until both schema locks are acquired, and a cascade delete blocks new cross-schema moves
   * until it finishes. Without this catalog fence, a move that holds schema A could deadlock
   * against a cascade that holds the catalog and is waiting for schema A.
   */
  void lockCatalogForEntityWrite(NameIdentifier entityIdentifier, Long catalogId, Long metalakeId) {
    String catalogName = entityIdentifier.namespace().level(1);
    OccWriteSupport.lockParentForChildWrite(
        catalogName,
        Entity.EntityType.CATALOG,
        () ->
            SessionUtils.getWithoutCommit(
                CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaByIdForShare(catalogId)),
        null,
        current ->
            Objects.equals(current.getCatalogName(), catalogName)
                && Objects.equals(current.getMetalakeId(), metalakeId));
  }

  void lockSchemaForEntityWrite(
      NameIdentifier entityIdentifier,
      Long observedSchemaId,
      Long observedCatalogId,
      Long observedMetalakeId) {
    NameIdentifier schemaIdentifier = NameIdentifierUtil.getSchemaIdentifier(entityIdentifier);
    OccWriteSupport.lockParentForChildWrite(
        schemaIdentifier.name(),
        Entity.EntityType.SCHEMA,
        () ->
            SessionUtils.getWithoutCommit(
                SchemaMetaMapper.class,
                mapper -> mapper.selectSchemaMetaByIdForShare(observedSchemaId)),
        SchemaMetaService::physicalToLogicalSchemaPO,
        current ->
            Objects.equals(current.getSchemaName(), schemaIdentifier.name())
                && Objects.equals(current.getCatalogId(), observedCatalogId)
                && Objects.equals(current.getMetalakeId(), observedMetalakeId));
  }

  /**
   * Decides which error a failed compare-and-set should report. The write matched no row either
   * because somebody else changed the schema, which is a conflict, or because the schema was
   * deleted or renamed away, which is a missing entity.
   */
  private RuntimeException schemaWriteFailure(
      NameIdentifier identifier, SchemaPO observedSchemaPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.SCHEMA,
        () ->
            SessionUtils.getWithoutCommit(
                SchemaMetaMapper.class,
                mapper -> mapper.selectSchemaMetaByIdForUpdate(observedSchemaPO.getSchemaId())),
        SchemaMetaService::physicalToLogicalSchemaPO,
        current ->
            Objects.equals(current.getSchemaName(), observedSchemaPO.getSchemaName())
                && Objects.equals(current.getCatalogId(), observedSchemaPO.getCatalogId())
                && Objects.equals(current.getMetalakeId(), observedSchemaPO.getMetalakeId()));
  }

  /**
   * Soft-deletes the nested schemas below the dropped one, each guarded by the version read in the
   * same transaction.
   */
  private void deleteDescendantSchemasWithVersions(
      NameIdentifier schemaIdentifier, List<SchemaPO> descendants) {
    OccWriteSupport.deleteChildrenWithVersions(
        schemaIdentifier,
        Entity.EntityType.SCHEMA,
        Entity.EntityType.SCHEMA,
        descendants,
        children ->
            SessionUtils.getWithoutCommit(
                SchemaMetaMapper.class,
                mapper -> mapper.softDeleteSchemaMetasWithVersion(children)));
  }

  private void deleteSchemaFunctions(NameIdentifier schemaIdentifier, Long schemaId) {
    try {
      // Resolve by ID because the schema row has already been soft-deleted in this transaction.
      List<FunctionPO> functions =
          SessionUtils.getWithoutCommit(
              FunctionMetaMapper.class, mapper -> mapper.listFunctionPOsBySchemaId(schemaId));
      Namespace namespace =
          NamespaceUtil.ofFunction(
              schemaIdentifier.namespace().level(0),
              schemaIdentifier.namespace().level(1),
              schemaIdentifier.name());
      for (FunctionPO function : functions) {
        NameIdentifier functionIdentifier = NameIdentifier.of(namespace, function.functionName());
        FunctionMetaService.getInstance().deleteFunctionWithVersion(functionIdentifier, function);
        EntityChangeLogWriter.append(
            functionIdentifier, Entity.EntityType.FUNCTION, OperateType.DROP);
      }
    } catch (NoSuchEntityException e) {
      // A missing function must not be reported as a missing schema. Other failures, including
      // optimistic conflicts, retain their original exception type.
      throw new RuntimeException("Failed to delete functions under schema " + schemaIdentifier, e);
    }
  }

  /** Checks that no active schema or metadata object is left below the schema. */
  private void checkSchemaIsEmpty(NameIdentifier identifier, SchemaPO schemaPO) {
    boolean hasDescendantSchemas = !listDescendantSchemaPOs(schemaPO).isEmpty();
    // A non-cascade delete only needs to know whether any direct child exists. Asking the database
    // for one literal avoids building every child PO and loading its version details while the
    // schema delete lock is held.
    boolean hasDirectChild =
        SessionUtils.getWithoutCommit(
                SchemaMetaMapper.class,
                mapper -> mapper.selectActiveChildBySchemaId(schemaPO.getSchemaId()))
            != null;
    if (hasDescendantSchemas || hasDirectChild) {
      throw new NonEmptyEntityException(
          "Entity %s has sub-entities, you should remove sub-entities first", identifier);
    }
  }

  /**
   * Collects every HierarchicalSchema descendant of the target schema. The {@link SchemaPO} arrives
   * in logical form (e.g. {@code A:B}); {@link HierarchicalConversionPOStorageOps} translates to
   * storage form before running the SQL prefix match.
   */
  private List<SchemaPO> listDescendantSchemaPOs(SchemaPO schemaPO) {
>>>>>>> c816482b8 ([#14119] fix(fileset): Preserve metadata on cascade cleanup failure (#14120))
    List<SchemaPO> matched =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                ops.listPOsByNamePrefix(mapper, schemaPO.getCatalogId(), schemaPO.getSchemaName()));
    if (matched == null || matched.isEmpty()) {
      return Collections.emptyList();
    }
    return matched.stream().map(SchemaPO::getSchemaId).collect(Collectors.toList());
  }

  private void fillSchemaPOBuilderParentEntityId(SchemaPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkSchema(namespace);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.CATALOG);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.entityId());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetSchemaByIdentifier")
  public List<SchemaEntity> batchGetSchemaByIdentifier(List<NameIdentifier> identifiers) {

    NameIdentifier firstIdent = identifiers.get(0);
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(firstIdent);
    List<String> schemaNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class,
        mapper -> {
          List<SchemaPO> schemaPOs =
              ops.listPOs(
                  mapper,
                  Namespace.of(catalogIdent.namespace().levels()[0], catalogIdent.name()),
                  schemaNames);
          return POConverters.fromSchemaPOs(schemaPOs, firstIdent.namespace());
        });
  }

  public BasePOStorageOps<SchemaPO, SchemaMetaMapper> ops() {
    return ops;
  }

  private static long nextIdForNestedAncestor() {
    IdGenerator generator = GravitinoEnv.getInstance().idGenerator();
    if (generator == null) {
      throw new IllegalStateException(
          "IdGenerator is not initialized in GravitinoEnv; ensure it is set up before inserting nested schemas");
    }
    return generator.nextId();
  }

  private static SchemaPO physicalToLogicalSchemaPO(SchemaPO po) {
    String name = po.getSchemaName();
    if (name == null || !name.contains(HierarchicalSchemaUtil.physicalSeparator())) {
      return po;
    }
    return copySchemaPOWithName(
        po,
        HierarchicalSchemaUtil.physicalToLogical(name, HierarchicalSchemaUtil.schemaSeparator()));
  }

  private static SchemaPO logicalToPhysicalSchemaPO(SchemaPO po) {
    String name = po.getSchemaName();
    if (name == null || !name.contains(HierarchicalSchemaUtil.schemaSeparator())) {
      return po;
    }
    return copySchemaPOWithName(
        po,
        HierarchicalSchemaUtil.logicalToPhysical(name, HierarchicalSchemaUtil.schemaSeparator()));
  }

  private static SchemaPO copySchemaPOWithName(SchemaPO po, String name) {
    return SchemaPO.builder()
        .withSchemaId(po.getSchemaId())
        .withSchemaName(name)
        .withMetalakeId(po.getMetalakeId())
        .withCatalogId(po.getCatalogId())
        .withSchemaComment(po.getSchemaComment())
        .withProperties(po.getProperties())
        .withAuditInfo(po.getAuditInfo())
        .withCurrentVersion(po.getCurrentVersion())
        .withLastVersion(po.getLastVersion())
        .withDeletedAt(po.getDeletedAt())
        .build();
  }
}
