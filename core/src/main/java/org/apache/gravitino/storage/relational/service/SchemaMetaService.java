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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.HierarchicalSchemaUtil;
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
import org.apache.gravitino.storage.RandomIdGenerator;
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
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** The service class for schema metadata. It provides the basic database operations for schema. */
public class SchemaMetaService {
  private static final SchemaMetaService INSTANCE = new SchemaMetaService();

  public static SchemaMetaService getInstance() {
    return INSTANCE;
  }

  private SchemaMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSchemaPOByCatalogIdAndName")
  public SchemaPO getSchemaPOByCatalogIdAndName(Long catalogId, String schemaName) {
    SchemaPO schemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaMetaByCatalogIdAndName(catalogId, schemaName));

    if (schemaPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          schemaName);
    }
    return schemaPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName")
  public SchemaIds getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
      String metalakeName, String catalogName, String schemaName) {
    SchemaIds schemaIds =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                    metalakeName, catalogName, schemaName));

    if (schemaIds == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          schemaName);
    }

    return schemaIds;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSchemaIdByCatalogIdAndName")
  public Long getSchemaIdByCatalogIdAndName(Long catalogId, String schemaName) {
    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));

    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          schemaName);
    }
    return schemaId;
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
      // Convert the logical entity name to physical before building and storing the PO.
      SchemaEntity physicalLeaf = toPhysicalEntity(schemaEntity);
      NameIdentifierUtil.checkSchema(physicalLeaf.nameIdentifier());

      String separator = HierarchicalSchemaUtil.schemaSeparator();
      String logicalLeaf = normalizeToLogicalSchemaName(schemaEntity.name());
      List<SchemaEntity> rowsToInsert = new ArrayList<>();
      if (!HierarchicalSchemaUtil.isHierarchical(logicalLeaf, separator)) {
        rowsToInsert.add(physicalLeaf);
      } else {
        for (String ancestorLogical :
            HierarchicalSchemaUtil.getAncestorNames(logicalLeaf, separator)) {
          SchemaEntity ancestor =
              SchemaEntity.builder()
                  .withId(nextIdForNestedAncestor())
                  .withName(ancestorLogical)
                  .withNamespace(schemaEntity.namespace())
                  .withComment(null)
                  .withProperties(Collections.emptyMap())
                  .withAuditInfo(schemaEntity.auditInfo())
                  .build();
          rowsToInsert.add(toPhysicalEntity(ancestor));
        }
        SchemaEntity leafWithLogicalName =
            SchemaEntity.builder()
                .withId(schemaEntity.id())
                .withName(logicalLeaf)
                .withNamespace(schemaEntity.namespace())
                .withComment(schemaEntity.comment())
                .withProperties(
                    schemaEntity.properties() == null
                        ? Collections.emptyMap()
                        : schemaEntity.properties())
                .withAuditInfo(schemaEntity.auditInfo())
                .build();
        rowsToInsert.add(toPhysicalEntity(leafWithLogicalName));
      }

      Namespace namespace = physicalLeaf.namespace();
      SessionUtils.doWithCommit(
          SchemaMetaMapper.class,
          mapper -> {
            String metalakeName = namespace.level(0);
            String catalogName = namespace.level(1);
            List<SchemaPO> posToInsert = new ArrayList<>();
            int n = rowsToInsert.size();
            if (n > 1) {
              List<SchemaEntity> ancestors = rowsToInsert.subList(0, n - 1);
              List<String> ancestorPhysicalNames =
                  ancestors.stream().map(SchemaEntity::name).collect(Collectors.toList());
              List<SchemaPO> existingAncestors =
                  mapper.batchSelectSchemaByIdentifier(
                      metalakeName, catalogName, ancestorPhysicalNames);
              Set<String> existingNames =
                  existingAncestors.stream()
                      .map(SchemaPO::getSchemaName)
                      .collect(Collectors.toSet());
              for (SchemaEntity row : ancestors) {
                if (existingNames.contains(row.name())) {
                  continue;
                }
                SchemaPO.Builder builder = SchemaPO.builder();
                fillSchemaPOBuilderParentEntityId(builder, row.namespace());
                posToInsert.add(POConverters.initializeSchemaPOWithVersion(row, builder));
              }
            }
            SchemaEntity leafRow = rowsToInsert.get(n - 1);
            SchemaPO.Builder leafBuilder = SchemaPO.builder();
            fillSchemaPOBuilderParentEntityId(leafBuilder, leafRow.namespace());
            posToInsert.add(POConverters.initializeSchemaPOWithVersion(leafRow, leafBuilder));
            if (overwrite) {
              mapper.batchInsertSchemaMetaOnDuplicateKeyUpdate(posToInsert);
            } else {
              mapper.batchInsertSchemaMeta(posToInsert);
            }
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
    // Identifier carries storage-form schema segment at JDBCBackend boundary; expose logical entity
    // to updater.
    SchemaPO oldSchemaPO = getSchemaPOByIdentifier(identifier);
    SchemaEntity oldSchemaEntity =
        toLogicalEntity(POConverters.fromSchemaPO(oldSchemaPO, identifier.namespace()));
    SchemaEntity newEntity = (SchemaEntity) updater.apply((E) oldSchemaEntity);
    Preconditions.checkArgument(
        Objects.equals(oldSchemaEntity.id(), newEntity.id()),
        "The updated schema entity id: %s should be same with the schema entity id before: %s",
        newEntity.id(),
        oldSchemaEntity.id());

    // Convert the updated logical entity to physical before persisting.
    SchemaEntity physicalNewEntity = toPhysicalEntity(newEntity);
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
                          mapper.updateSchemaMeta(
                              POConverters.updateSchemaPOWithVersion(
                                  oldSchemaPO, physicalNewEntity),
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
      // Return the logical entity (as seen by callers above the EntityStore layer).
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

    String schemaSegmentPhysical = identifier.name();
    SchemaPO schemaPO = getSchemaPOByIdentifier(identifier);
    Long schemaId = schemaPO.getSchemaId();
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String auditSchemaName = normalizeToLogicalSchemaName(schemaSegmentPhysical);
    String schemaFullName =
        NameIdentifierUtil.ofSchema(metalakeName, catalogName, auditSchemaName).toString();

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
                      schemaSegmentPhysical));
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
                      schemaSegmentPhysical));
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
                      schemaSegmentPhysical));
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
                      schemaSegmentPhysical));
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
    return schemaPOFetcher().apply(identifier);
  }

  private SchemaPO getSchemaByFullQualifiedName(
      String metalakeName, String catalogName, String schemaName) {
    SchemaPO schemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaByFullQualifiedName(metalakeName, catalogName, schemaName));
    if (schemaPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          EntityType.CATALOG.name().toLowerCase(),
          schemaName);
    }

    return schemaPO;
  }

  private List<SchemaPO> listSchemaPOs(Namespace namespace) {
    return schemaListFetcher().apply(namespace);
  }

  private List<SchemaPO> listSchemaPOsByCatalogId(Namespace namespace) {
    Long catalogId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.CATALOG);

    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsByCatalogId(catalogId));
  }

  private List<SchemaPO> listSchemaPOsByFullQualifiedName(Namespace namespace) {
    String[] namespaceLevels = namespace.levels();
    List<SchemaPO> schemaPOs =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.listSchemaPOsByFullQualifiedName(namespaceLevels[0], namespaceLevels[1]));
    if (schemaPOs.isEmpty() || schemaPOs.get(0).getCatalogId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          namespaceLevels[1]);
    }
    return schemaPOs.stream().filter(po -> po.getSchemaId() != null).collect(Collectors.toList());
  }

  private SchemaPO getSchemaPOByCatalogId(NameIdentifier identifier) {
    Long catalogId =
        EntityIdService.getEntityId(
            NameIdentifier.of(identifier.namespace().levels()), Entity.EntityType.CATALOG);
    return getSchemaPOByCatalogIdAndName(catalogId, identifier.name());
  }

  private SchemaPO getSchemaPOByFullQualifiedName(NameIdentifier identifier) {
    String[] namespaceLevels = identifier.namespace().levels();
    SchemaPO schemaPO =
        getSchemaByFullQualifiedName(namespaceLevels[0], namespaceLevels[1], identifier.name());

    if (schemaPO.getCatalogId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          namespaceLevels[1]);
    }

    if (schemaPO.getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          identifier.name());
    }

    return schemaPO;
  }

  private Function<Namespace, List<SchemaPO>> schemaListFetcher() {
    // If cache is enabled, we can use catalog id to fetch schemas faster or else use full qualified
    // name to join several tables to get the schema list.
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::listSchemaPOsByCatalogId
        : this::listSchemaPOsByFullQualifiedName;
  }

  private Function<NameIdentifier, SchemaPO> schemaPOFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::getSchemaPOByCatalogId
        : this::getSchemaPOByFullQualifiedName;
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

    return SessionUtils.doWithCommitAndFetchResult(
        SchemaMetaMapper.class,
        mapper -> {
          List<SchemaPO> schemaPOs =
              mapper.batchSelectSchemaByIdentifier(
                  catalogIdent.namespace().level(0), catalogIdent.name(), schemaNames);
          return POConverters.fromSchemaPOs(schemaPOs, firstIdent.namespace());
        });
  }

  // ---------------------------------------------------------------------------
  // Helpers: logical ↔ physical schema name conversion at the PO boundary
  // ---------------------------------------------------------------------------

  /**
   * Returns the logical schema path for hierarchy expansion. Values already using the external
   * separator are unchanged; values stored with only the internal physical separator are converted
   * to logical form.
   */
  private String normalizeToLogicalSchemaName(String schemaName) {
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    if (HierarchicalSchemaUtil.isHierarchical(schemaName, separator)) {
      return schemaName;
    }
    if (schemaName != null && schemaName.contains(HierarchicalSchemaUtil.physicalSeparator())) {
      return toLogicalSchemaName(schemaName);
    }
    return schemaName;
  }

  /**
   * Converts a logical schema name (e.g. {@code "A:B:C"}) to the physical internal form (e.g.
   * {@code "A\u0001B\u0001C"}) used in the database. Non-HierarchicalSchema names are returned
   * unchanged.
   */
  private String toPhysicalSchemaName(String logicalName) {
    return HierarchicalSchemaUtil.logicalToPhysical(
        logicalName, HierarchicalSchemaUtil.schemaSeparator());
  }

  /**
   * Converts a physical schema name (e.g. {@code "A\u0001B\u0001C"}) back to the logical separator
   * form (e.g. {@code "A:B:C"}). Non-HierarchicalSchema names are returned unchanged.
   */
  private String toLogicalSchemaName(String physicalName) {
    return HierarchicalSchemaUtil.physicalToLogical(
        physicalName, HierarchicalSchemaUtil.schemaSeparator());
  }

  /**
   * Returns a {@link SchemaEntity} whose {@code name()} is the logical representation. If the PO
   * stored a physical name (contains the internal physical separator), it is converted back to the
   * logical separator form. Otherwise the original entity is returned unchanged.
   */
  private SchemaEntity toLogicalEntity(SchemaEntity entity) {
    String logicalName = toLogicalSchemaName(entity.name());
    if (logicalName.equals(entity.name())) {
      return entity;
    }
    return SchemaEntity.builder()
        .withId(entity.id())
        .withName(logicalName)
        .withNamespace(entity.namespace())
        .withComment(entity.comment())
        .withProperties(entity.properties())
        .withAuditInfo(entity.auditInfo())
        .build();
  }

  /**
   * Returns a {@link SchemaEntity} whose {@code name()} is the physical representation suitable for
   * storage. If the entity carries a logical name (contains the separator), it is converted to the
   * physical internal form. Otherwise the original entity is returned unchanged.
   */
  private SchemaEntity toPhysicalEntity(SchemaEntity entity) {
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    if (!HierarchicalSchemaUtil.isHierarchical(entity.name(), separator)) {
      return entity;
    }
    String physicalName = toPhysicalSchemaName(entity.name());
    return SchemaEntity.builder()
        .withId(entity.id())
        .withName(physicalName)
        .withNamespace(entity.namespace())
        .withComment(entity.comment())
        .withProperties(entity.properties())
        .withAuditInfo(entity.auditInfo())
        .build();
  }

  /**
   * Nested schema materialization needs extra ids for ancestor rows. Relational tests set {@link
   * GravitinoEnv} config without a full server {@link IdGenerator}; fall back for that case.
   */
  private static long nextIdForNestedAncestor() {
    IdGenerator generator = GravitinoEnv.getInstance().idGenerator();
    return generator != null ? generator.nextId() : RandomIdGenerator.INSTANCE.nextId();
  }
}
