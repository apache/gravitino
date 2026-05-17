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
        new HierarchicalConventionPOStorageOps<>(
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
      // logical form. HierarchicalConventionPOStorageOps.batchInsertPOs applies its write
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
              // The mapper queries storage (physical) form names directly, so convert before
              // the lookup and compare in physical form.
              List<String> ancestorPhysicalNames =
                  rowsToInsert.subList(0, n - 1).stream()
                      .map(SchemaEntity::name)
                      .map(name -> HierarchicalSchemaUtil.logicalToPhysical(name, logicalSep))
                      .collect(Collectors.toList());
              Set<String> existingPhysicalNames =
                  mapper
                      .batchSelectSchemaByIdentifier(
                          ancestorNs.level(0), ancestorNs.level(1), ancestorPhysicalNames)
                      .stream()
                      .map(SchemaPO::getSchemaName)
                      .collect(Collectors.toSet());
              for (SchemaEntity row : rowsToInsert.subList(0, n - 1)) {
                String physicalName =
                    HierarchicalSchemaUtil.logicalToPhysical(row.name(), logicalSep);
                if (existingPhysicalNames.contains(physicalName)) {
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
    NameIdentifierUtil.checkSchema(identifier);

    String schemaName = identifier.name();
    SchemaPO schemaPO = getSchemaPOByIdentifier(identifier);
    Long schemaId = schemaPO.getSchemaId();
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaFullName =
        NameIdentifierUtil.ofSchema(metalakeName, catalogName, schemaName).toString();

    if (cascade) {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TableMetaMapper.class, mapper -> mapper.softDeleteTableMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TableColumnMapper.class, mapper -> mapper.softDeleteColumnsBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetMetaMapper.class,
                  mapper -> mapper.softDeleteFilesetMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetVersionMapper.class,
                  mapper -> mapper.softDeleteFilesetVersionsBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TopicMetaMapper.class, mapper -> mapper.softDeleteTopicMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionVersionMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class, mapper -> mapper.softDeleteOwnerRelBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper -> mapper.softDeleteObjectRelsBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TagMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeleteTagMetadataObjectRelsBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeletePolicyMetadataObjectRelsBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper -> mapper.softDeleteModelVersionAliasRelsBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.softDeleteModelVersionMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelMetaMapper.class, mapper -> mapper.softDeleteModelMetasBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  StatisticMetaMapper.class,
                  mapper -> mapper.softDeleteStatisticsBySchemaId(schemaId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ViewMetaMapper.class, mapper -> mapper.softDeleteViewMetasBySchemaId(schemaId)),
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

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId)),
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
    return true;
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
