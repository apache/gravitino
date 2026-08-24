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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.helper.SchemaIds;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
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
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
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
      String metalakeName = schemaEntity.namespace().level(0);
      String catalogName = schemaEntity.namespace().level(1);
      CatalogPO catalogPO =
          CatalogMetaService.getInstance().getCatalogPOByName(metalakeName, catalogName);
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

      // Everything below runs in one transaction, and it starts by locking the parent catalog row.
      // That lock is what stops a catalog drop from running at the same time as this insert. A
      // plain name is enough with a shared lock; a nested name needs an exclusive one, see
      // lockCatalogForSchemaCreate.
      SessionUtils.doMultipleWithCommit(
          () -> lockCatalogForSchemaCreate(catalogPO, rowsToInsert.size() > 1),
          () ->
              SessionUtils.doWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> {
                    int n = rowsToInsert.size();
                    List<SchemaPO> missingAncestorPOs = new ArrayList<>();
                    if (n > 1) {
                      // Only insert the ancestors that are not there yet. Reading them inside the
                      // transaction is safe because the exclusive catalog lock is already held, so
                      // no other request can add the same ancestor between this read and the
                      // insert below.
                      SchemaEntity firstAncestor = rowsToInsert.get(0);
                      Namespace ancestorNs = firstAncestor.namespace();
                      List<String> ancestorNames =
                          rowsToInsert.subList(0, n - 1).stream()
                              .map(SchemaEntity::name)
                              .collect(Collectors.toList());
                      Map<String, SchemaPO> existingAncestors =
                          ops.listPOs(mapper, ancestorNs, ancestorNames).stream()
                              .collect(
                                  Collectors.toMap(SchemaPO::getSchemaName, Function.identity()));
                      for (SchemaEntity row : rowsToInsert.subList(0, n - 1)) {
                        SchemaPO existingAncestor = existingAncestors.get(row.name());
                        if (existingAncestor != null) {
                          continue;
                        }
                        SchemaPO.Builder builder = newSchemaPOBuilder(catalogPO);
                        missingAncestorPOs.add(
                            POConverters.initializeSchemaPOWithVersion(row, builder));
                      }
                    }
                    if (!missingAncestorPOs.isEmpty()) {
                      ops.batchInsertPOs(mapper, missingAncestorPOs, false);
                    }
                    // The schema the caller actually asked for. Ancestors above are filled in
                    // silently, but this row must obey the caller's choice: with overwrite off, a
                    // name that is already taken fails instead of replacing the existing schema.
                    SchemaEntity leafRow = rowsToInsert.get(n - 1);
                    SchemaPO leafPO =
                        POConverters.initializeSchemaPOWithVersion(
                            leafRow, newSchemaPOBuilder(catalogPO));
                    ops.batchInsertPOs(mapper, Collections.singletonList(leafPO), overwrite);
                  }));
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

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            // The UPDATE only matches the row while it still carries the version read above, and it
            // writes the next version. Two servers that started from the same schema therefore
            // cannot both apply their change: the slower one updates no row.
            int updated =
                SessionUtils.getWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper ->
                        ops.updatePO(
                            mapper,
                            POConverters.updateSchemaPOWithVersion(oldSchemaPO, newEntity),
                            oldSchemaPO));
            if (updated == 0) {
              // Zero rows has two possible causes: someone else changed the schema, or the schema
              // is gone. schemaWriteFailure tells them apart and picks the right error.
              throw schemaWriteFailure(identifier, oldSchemaPO);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.SCHEMA, newEntity.nameIdentifier().toString());
      throw re;
    }

    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSchema")
  public boolean deleteSchema(NameIdentifier identifier, boolean cascade) {
    NameIdentifierUtil.checkSchema(identifier);

    SchemaPO schemaPO = getSchemaPOByIdentifier(identifier);
    Long schemaId = schemaPO.getSchemaId();

    if (cascade) {
      AtomicReference<List<Long>> schemaIds = new AtomicReference<>();
      SessionUtils.doMultipleWithCommit(
          () -> {
            // Take the parent catalog lock first, then delete this schema, and only then look at
            // its descendants. Schema creation takes the same catalog lock, so once we hold it no
            // schema can appear or disappear under us. Every overlapping drop grabs the locks in
            // this same order, which is what keeps two cascades from deadlocking each other.
            lockCatalogForSchemaDelete(identifier, schemaPO);
            deleteSchemaWithVersion(identifier, schemaPO);
            List<SchemaPO> descendants = listDescendantSchemaPOs(schemaPO);
            deleteDescendantSchemasWithVersions(identifier, descendants);
            List<Long> ids = new ArrayList<>(descendants.size() + 1);
            ids.add(schemaId);
            descendants.stream().map(SchemaPO::getSchemaId).forEach(ids::add);
            schemaIds.set(ids);
          },
          () ->
              SessionUtils.doWithoutCommit(
                  TableMetaMapper.class,
                  mapper -> mapper.softDeleteTableMetasBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  TableColumnMapper.class,
                  mapper -> mapper.softDeleteColumnsBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetMetaMapper.class,
                  mapper -> mapper.softDeleteFilesetMetasBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetVersionMapper.class,
                  mapper -> mapper.softDeleteFilesetVersionsBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  TopicMetaMapper.class,
                  mapper -> mapper.softDeleteTopicMetasBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionMetasBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionVersionMetasBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class,
                  mapper -> mapper.softDeleteOwnerRelBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper -> mapper.softDeleteObjectRelsBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  TagMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeleteTagMetadataObjectRelsBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeletePolicyMetadataObjectRelsBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper -> mapper.softDeleteModelVersionAliasRelsBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.softDeleteModelVersionMetasBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelMetaMapper.class,
                  mapper -> mapper.softDeleteModelMetasBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  StatisticMetaMapper.class,
                  mapper -> mapper.softDeleteStatisticsBySchemaIds(schemaIds.get())),
          () ->
              SessionUtils.doWithoutCommit(
                  ViewMetaMapper.class,
                  mapper -> mapper.softDeleteViewMetasBySchemaIds(schemaIds.get())));
    } else {
      SessionUtils.doMultipleWithCommit(
          () -> {
            // Delete the schema first and check that it was empty afterwards. The order matters:
            // the delete locks the schema row, and every child write locks that same row first, so
            // a table or view being created either lands before this delete and shows up in the
            // check, or it waits for this transaction. Checking first would leave a gap for a child
            // to appear in between. A non-empty result throws, which rolls the delete back.
            lockCatalogForSchemaDelete(identifier, schemaPO);
            deleteSchemaWithVersion(identifier, schemaPO);
            checkSchemaIsEmpty(identifier, schemaPO);
          },
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
                          schemaId, MetadataObject.Type.SCHEMA.name())));
    }
    return true;
  }

  /**
   * Soft-deletes the schema only while it still carries the version the caller read. A drop that
   * lost the race must not delete a schema it never looked at.
   */
  private void deleteSchemaWithVersion(NameIdentifier identifier, SchemaPO observedSchemaPO) {
    int deleted =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                    observedSchemaPO.getSchemaId(), observedSchemaPO.getCurrentVersion()));
    if (deleted == 0) {
      throw schemaWriteFailure(identifier, observedSchemaPO);
    }
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
   * Holds the parent catalog row for the rest of the transaction, so a schema cannot be created
   * below a catalog that is being dropped. Dropping a catalog locks this same row, so the two can
   * never run at the same time: the loser either finds the catalog gone or inserts below a catalog
   * that is still there.
   *
   * <p>A plain schema name only needs a shared lock, so many schemas can be created under one
   * catalog at once. A nested name is different: this request may have to create the missing
   * ancestors, and two requests can both find the same ancestor missing and both insert it. A
   * shared lock does not stop that, so the ancestor case takes an exclusive lock and serializes
   * every other schema create under the catalog until it finishes.
   *
   * <p>The name and the metalake are compared again because the caller looked the catalog up by
   * name: if the row now has another name, the catalog named in the request no longer exists.
   */
  private void lockCatalogForSchemaCreate(
      CatalogPO observedCatalogPO, boolean createsImplicitAncestors) {
    CatalogPO currentCatalogPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper ->
                createsImplicitAncestors
                    ? mapper.selectCatalogMetaByIdForUpdate(observedCatalogPO.getCatalogId())
                    : mapper.selectCatalogMetaByIdForShare(observedCatalogPO.getCatalogId()));
    if (currentCatalogPO == null
        || !Objects.equals(currentCatalogPO.getCatalogName(), observedCatalogPO.getCatalogName())
        || !Objects.equals(currentCatalogPO.getMetalakeId(), observedCatalogPO.getMetalakeId())) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          observedCatalogPO.getCatalogName());
    }
  }

  /**
   * Holds the parent catalog row while a schema is dropped. The lock is exclusive here, because a
   * drop removes descendants and must not run next to another drop or create under the same
   * catalog. Taking the catalog before any schema row also gives every drop the same lock order, so
   * two overlapping cascades cannot deadlock.
   */
  private void lockCatalogForSchemaDelete(NameIdentifier identifier, SchemaPO observedSchemaPO) {
    CatalogPO currentCatalogPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogMetaByIdForUpdate(observedSchemaPO.getCatalogId()));
    if (currentCatalogPO == null
        || !Objects.equals(currentCatalogPO.getCatalogName(), identifier.namespace().level(1))
        || !Objects.equals(currentCatalogPO.getMetalakeId(), observedSchemaPO.getMetalakeId())) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          identifier.namespace().level(1));
    }
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

  private void lockSchemaForEntityWrite(
      NameIdentifier entityIdentifier,
      Long observedSchemaId,
      Long observedCatalogId,
      Long observedMetalakeId) {
    NameIdentifier schemaIdentifier = NameIdentifierUtil.getSchemaIdentifier(entityIdentifier);
    SchemaPO currentSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaMetaByIdForShare(observedSchemaId));
    if (currentSchemaPO != null) {
      currentSchemaPO = physicalToLogicalSchemaPO(currentSchemaPO);
    }
    if (currentSchemaPO == null
        || !Objects.equals(currentSchemaPO.getSchemaName(), schemaIdentifier.name())
        || !Objects.equals(currentSchemaPO.getCatalogId(), observedCatalogId)
        || !Objects.equals(currentSchemaPO.getMetalakeId(), observedMetalakeId)) {
      throw noSuchSchemaException(schemaIdentifier);
    }
  }

  /**
   * Decides which error a failed compare-and-set should report. The write matched no row either
   * because somebody else changed the schema, which is a conflict, or because the schema was
   * deleted or renamed away, which is a missing entity.
   */
  private RuntimeException schemaWriteFailure(
      NameIdentifier identifier, SchemaPO observedSchemaPO) {
    // Sessions run at READ_COMMITTED, so a plain read would already see the latest committed row.
    // The locking read additionally waits for a writer that is still in flight, so a delete or
    // rename that has not committed yet is reported as a missing schema instead of as a stale
    // version conflict. The lock is taken on the error path of a transaction that is about to roll
    // back.
    SchemaPO currentSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaMetaByIdForUpdate(observedSchemaPO.getSchemaId()));
    if (currentSchemaPO == null) {
      return noSuchSchemaException(identifier);
    }
    currentSchemaPO = physicalToLogicalSchemaPO(currentSchemaPO);
    if (!Objects.equals(currentSchemaPO.getSchemaName(), observedSchemaPO.getSchemaName())
        || !Objects.equals(currentSchemaPO.getCatalogId(), observedSchemaPO.getCatalogId())
        || !Objects.equals(currentSchemaPO.getMetalakeId(), observedSchemaPO.getMetalakeId())) {
      return noSuchSchemaException(identifier);
    }
    return ExceptionUtils.concurrentModification(Entity.EntityType.SCHEMA, identifier);
  }

  private NoSuchEntityException noSuchSchemaException(NameIdentifier identifier) {
    return new NoSuchEntityException(
        NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
        Entity.EntityType.SCHEMA.name().toLowerCase(),
        identifier.name());
  }

  /**
   * Soft-deletes the nested schemas below the dropped one, each guarded by the version read in the
   * same transaction.
   */
  private void deleteDescendantSchemasWithVersions(
      NameIdentifier schemaIdentifier, List<SchemaPO> descendants) {
    if (descendants.isEmpty()) {
      return;
    }
    int deleted =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.softDeleteSchemaMetasWithVersion(descendants));
    // A smaller count means one of these schemas was altered by a request that did not take the
    // catalog lock. Never commit half a cascade: roll the whole transaction back instead.
    if (deleted != descendants.size()) {
      throw ExceptionUtils.concurrentChildModification(
          Entity.EntityType.SCHEMA, Entity.EntityType.SCHEMA, schemaIdentifier);
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
    List<SchemaPO> matched =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                ops.listPOsByNamePrefix(mapper, schemaPO.getCatalogId(), schemaPO.getSchemaName()));
    if (matched == null || matched.isEmpty()) {
      return Collections.emptyList();
    }
    return matched.stream()
        .filter(po -> !po.getSchemaId().equals(schemaPO.getSchemaId()))
        .collect(Collectors.toList());
  }

  private SchemaPO.Builder newSchemaPOBuilder(CatalogPO catalogPO) {
    return SchemaPO.builder()
        .withMetalakeId(catalogPO.getMetalakeId())
        .withCatalogId(catalogPO.getCatalogId());
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
