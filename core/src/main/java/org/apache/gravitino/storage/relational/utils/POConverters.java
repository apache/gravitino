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

package org.apache.gravitino.storage.relational.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.ExtendedGroupPO;
import org.apache.gravitino.storage.relational.po.ExtendedUserPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.ModelVersionAliasRelPO;
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.utils.PrincipalUtils;

/** POConverters is a utility class to convert PO to Base and vice versa. */
public class POConverters {
  private static final long INIT_VERSION = 1L;
  private static final long DEFAULT_DELETED_AT = 0L;

  private POConverters() {}

  /**
   * Initialize MetalakePO
   *
   * @param baseMetalake BaseMetalake object
   * @return MetalakePO object with version initialized
   */
  public static MetalakePO initializeMetalakePOWithVersion(BaseMetalake baseMetalake) {
    try {
      return MetalakePO.builder()
          .withMetalakeId(baseMetalake.id())
          .withMetalakeName(baseMetalake.name())
          .withMetalakeComment(baseMetalake.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.auditInfo()))
          .withSchemaVersion(
              JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.getVersion()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update MetalakePO version
   *
   * @param oldMetalakePO the old MetalakePO object
   * @param newMetalake the new BaseMetalake object
   * @return MetalakePO object with updated version
   */
  public static MetalakePO updateMetalakePOWithVersion(
      MetalakePO oldMetalakePO, BaseMetalake newMetalake) {
    Long lastVersion = oldMetalakePO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return MetalakePO.builder()
          .withMetalakeId(newMetalake.id())
          .withMetalakeName(newMetalake.name())
          .withMetalakeComment(newMetalake.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newMetalake.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newMetalake.auditInfo()))
          .withSchemaVersion(
              JsonUtils.anyFieldMapper().writeValueAsString(newMetalake.getVersion()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link MetalakePO} to {@link BaseMetalake}
   *
   * @param metalakePO MetalakePO object
   * @return BaseMetalake object from MetalakePO object
   */
  public static BaseMetalake fromMetalakePO(MetalakePO metalakePO) {
    try {
      return BaseMetalake.builder()
          .withId(metalakePO.getMetalakeId())
          .withName(metalakePO.getMetalakeName())
          .withComment(metalakePO.getMetalakeComment())
          .withProperties(
              JsonUtils.anyFieldMapper().readValue(metalakePO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(metalakePO.getAuditInfo(), AuditInfo.class))
          .withVersion(
              JsonUtils.anyFieldMapper()
                  .readValue(metalakePO.getSchemaVersion(), SchemaVersion.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link MetalakePO} to list of {@link BaseMetalake}
   *
   * @param metalakePOs list of MetalakePO objects
   * @return list of BaseMetalake objects from list of MetalakePO objects
   */
  public static List<BaseMetalake> fromMetalakePOs(List<MetalakePO> metalakePOs) {
    return metalakePOs.stream().map(POConverters::fromMetalakePO).collect(Collectors.toList());
  }

  /**
   * Initialize CatalogPO
   *
   * @param catalogEntity the {@link CatalogEntity} containing source metadata for the catalog
   * @param metalakeId the identifier of the metalake environment to which this catalog belongs
   * @return {@link CatalogPO} object with version initialized
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static CatalogPO initializeCatalogPOWithVersion(
      CatalogEntity catalogEntity, Long metalakeId) {
    try {
      return CatalogPO.builder()
          .withCatalogId(catalogEntity.id())
          .withCatalogName(catalogEntity.name())
          .withMetalakeId(metalakeId)
          .withType(catalogEntity.getType().name())
          .withProvider(catalogEntity.getProvider())
          .withCatalogComment(catalogEntity.getComment())
          .withProperties(
              JsonUtils.anyFieldMapper().writeValueAsString(catalogEntity.getProperties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(catalogEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update CatalogPO version
   *
   * @param oldCatalogPO the existing {@link CatalogPO} containing the version to preserve
   * @param newCatalog the {@link CatalogEntity} with updated catalog metadata
   * @param metalakeId the identifier of the metalake environment for this catalog
   * @return {@code CatalogPO} object with updated version
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static CatalogPO updateCatalogPOWithVersion(
      CatalogPO oldCatalogPO, CatalogEntity newCatalog, Long metalakeId) {
    Long lastVersion = oldCatalogPO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return CatalogPO.builder()
          .withCatalogId(newCatalog.id())
          .withCatalogName(newCatalog.name())
          .withMetalakeId(metalakeId)
          .withType(newCatalog.getType().name())
          .withProvider(newCatalog.getProvider())
          .withCatalogComment(newCatalog.getComment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newCatalog.getProperties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newCatalog.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link CatalogPO} to {@link CatalogEntity}
   *
   * @param catalogPO CatalogPO object to be converted
   * @param namespace Namespace object to be associated with the catalog
   * @return CatalogEntity object from CatalogPO object
   */
  public static CatalogEntity fromCatalogPO(CatalogPO catalogPO, Namespace namespace) {
    try {
      return CatalogEntity.builder()
          .withId(catalogPO.getCatalogId())
          .withName(catalogPO.getCatalogName())
          .withNamespace(namespace)
          .withType(Catalog.Type.valueOf(catalogPO.getType()))
          .withProvider(catalogPO.getProvider())
          .withComment(catalogPO.getCatalogComment())
          .withProperties(
              JsonUtils.anyFieldMapper().readValue(catalogPO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(catalogPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link CatalogPO} to list of {@link CatalogEntity}
   *
   * @param catalogPOs list of CatalogPO objects
   * @param namespace Namespace object to be associated with the catalog
   * @return list of CatalogEntity objects from list of CatalogPO objects
   */
  public static List<CatalogEntity> fromCatalogPOs(
      List<CatalogPO> catalogPOs, Namespace namespace) {
    return catalogPOs.stream()
        .map(catalogPO -> POConverters.fromCatalogPO(catalogPO, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Initialize SchemaPO
   *
   * @param schemaEntity the {@link SchemaEntity} containing source data for the PO object
   * @param builder the {@link SchemaPO.Builder} used to assemble the {@link SchemaPO}
   * @return {@code CatalogPO} object with version initialized
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static SchemaPO initializeSchemaPOWithVersion(
      SchemaEntity schemaEntity, SchemaPO.Builder builder) {
    try {
      return builder
          .withSchemaId(schemaEntity.id())
          .withSchemaName(schemaEntity.name())
          .withSchemaComment(schemaEntity.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(schemaEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(schemaEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update SchemaPO version
   *
   * @param oldSchemaPO the old SchemaPO object
   * @param newSchema the new SchemaEntity object
   * @return SchemaPO object with updated version
   */
  public static SchemaPO updateSchemaPOWithVersion(SchemaPO oldSchemaPO, SchemaEntity newSchema) {
    Long lastVersion = oldSchemaPO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return SchemaPO.builder()
          .withSchemaId(oldSchemaPO.getSchemaId())
          .withSchemaName(newSchema.name())
          .withMetalakeId(oldSchemaPO.getMetalakeId())
          .withCatalogId(oldSchemaPO.getCatalogId())
          .withSchemaComment(newSchema.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newSchema.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newSchema.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link SchemaPO} to {@link SchemaEntity}
   *
   * @param schemaPO SchemaPO object to be converted
   * @param namespace Namespace object to be associated with the schema
   * @return SchemaEntity object from SchemaPO object
   */
  public static SchemaEntity fromSchemaPO(SchemaPO schemaPO, Namespace namespace) {
    try {
      return SchemaEntity.builder()
          .withId(schemaPO.getSchemaId())
          .withName(schemaPO.getSchemaName())
          .withNamespace(namespace)
          .withComment(schemaPO.getSchemaComment())
          .withProperties(JsonUtils.anyFieldMapper().readValue(schemaPO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(schemaPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link SchemaPO} to list of {@link SchemaEntity}
   *
   * @param schemaPOs list of SchemaPO objects
   * @param namespace Namespace object to be associated with the schema
   * @return list of SchemaEntity objects from list of SchemaPO objects
   */
  public static List<SchemaEntity> fromSchemaPOs(List<SchemaPO> schemaPOs, Namespace namespace) {
    return schemaPOs.stream()
        .map(schemaPO -> POConverters.fromSchemaPO(schemaPO, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Initialize TablePO
   *
   * @param tableEntity the {@link TableEntity} containing source data for the PO object
   * @param builder the {@link TablePO.Builder} used to assemble the {@link TablePO}
   * @return {@code TablePO} object with version initialized
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static TablePO initializeTablePOWithVersion(
      TableEntity tableEntity, TablePO.Builder builder) {
    try {
      return builder
          .withTableId(tableEntity.id())
          .withTableName(tableEntity.name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(tableEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update TablePO version
   *
   * @param oldTablePO the old TablePO object
   * @param newTable the new TableEntity object
   * @param needUpdateVersion whether need to update the version
   * @return TablePO object with updated version
   */
  public static TablePO updateTablePOWithVersion(
      TablePO oldTablePO, TableEntity newTable, boolean needUpdateVersion) {
    Long lastVersion;
    Long currentVersion;
    if (needUpdateVersion) {
      lastVersion = oldTablePO.getLastVersion() + 1;
      currentVersion = lastVersion;
    } else {
      lastVersion = oldTablePO.getLastVersion();
      currentVersion = oldTablePO.getCurrentVersion();
    }

    try {
      return TablePO.builder()
          .withTableId(oldTablePO.getTableId())
          .withTableName(newTable.name())
          .withMetalakeId(oldTablePO.getMetalakeId())
          .withCatalogId(oldTablePO.getCatalogId())
          .withSchemaId(oldTablePO.getSchemaId())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newTable.auditInfo()))
          .withCurrentVersion(currentVersion)
          .withLastVersion(lastVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link TablePO} to {@link TableEntity}
   *
   * @param tablePO TablePO object to be converted
   * @param namespace Namespace object to be associated with the table
   * @return TableEntity object from TablePO object
   */
  public static TableEntity fromTablePO(TablePO tablePO, Namespace namespace) {
    return fromTableAndColumnPOs(tablePO, Collections.emptyList(), namespace);
  }

  public static TableEntity fromTableAndColumnPOs(
      TablePO tablePO, List<ColumnPO> columnPOs, Namespace namespace) {
    try {
      return TableEntity.builder()
          .withId(tablePO.getTableId())
          .withName(tablePO.getTableName())
          .withNamespace(namespace)
          .withColumns(fromColumnPOs(columnPOs))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(tablePO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static ColumnEntity fromColumnPO(ColumnPO columnPO) {
    try {
      return ColumnEntity.builder()
          .withId(columnPO.getColumnId())
          .withName(columnPO.getColumnName())
          .withPosition(columnPO.getColumnPosition())
          .withDataType(JsonUtils.anyFieldMapper().readValue(columnPO.getColumnType(), Type.class))
          .withComment(columnPO.getColumnComment())
          .withAutoIncrement(
              ColumnPO.AutoIncrement.fromValue(columnPO.getAutoIncrement()).autoIncrement())
          .withNullable(ColumnPO.Nullable.fromValue(columnPO.getNullable()).nullable())
          .withDefaultValue(
              columnPO.getDefaultValue() == null
                  ? Column.DEFAULT_VALUE_NOT_SET
                  : DTOConverters.fromFunctionArg(
                      (FunctionArg)
                          JsonUtils.anyFieldMapper()
                              .readValue(columnPO.getDefaultValue(), Expression.class)))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(columnPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static List<ColumnEntity> fromColumnPOs(List<ColumnPO> columnPOs) {
    return columnPOs.stream().map(POConverters::fromColumnPO).collect(Collectors.toList());
  }

  public static ColumnPO initializeColumnPO(
      TablePO tablePO, ColumnEntity columnEntity, ColumnPO.ColumnOpType opType) {
    try {
      return ColumnPO.builder()
          .withColumnId(columnEntity.id())
          .withColumnName(columnEntity.name())
          .withColumnPosition(columnEntity.position())
          .withMetalakeId(tablePO.getMetalakeId())
          .withCatalogId(tablePO.getCatalogId())
          .withSchemaId(tablePO.getSchemaId())
          .withTableId(tablePO.getTableId())
          .withTableVersion(tablePO.getCurrentVersion())
          .withColumnType(JsonUtils.anyFieldMapper().writeValueAsString(columnEntity.dataType()))
          .withColumnComment(columnEntity.comment())
          .withNullable(ColumnPO.Nullable.fromBoolean(columnEntity.nullable()).value())
          .withAutoIncrement(
              ColumnPO.AutoIncrement.fromBoolean(columnEntity.autoIncrement()).value())
          .withDefaultValue(
              columnEntity.defaultValue() == null
                      || columnEntity.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET)
                  ? null
                  : JsonUtils.anyFieldMapper()
                      .writeValueAsString(DTOConverters.toFunctionArg(columnEntity.defaultValue())))
          .withColumnOpType(opType.value())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(columnEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static List<ColumnPO> initializeColumnPOs(
      TablePO tablePO, List<ColumnEntity> columnEntities, ColumnPO.ColumnOpType opType) {
    return columnEntities.stream()
        .map(columnEntity -> initializeColumnPO(tablePO, columnEntity, opType))
        .collect(Collectors.toList());
  }

  /**
   * Convert list of {@link TablePO} to list of {@link TableEntity}
   *
   * @param tablePOs list of TablePO objects
   * @param namespace Namespace object to be associated with the table
   * @return list of TableEntity objects from list of TablePO objects
   */
  public static List<TableEntity> fromTablePOs(List<TablePO> tablePOs, Namespace namespace) {
    return tablePOs.stream()
        .map(tablePO -> POConverters.fromTablePO(tablePO, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Initialize FilesetPO
   *
   * @param filesetEntity the source {@link FilesetEntity} object
   * @param builder the {@link FilesetPO.Builder} used to assemble the {@link FilesetPO}
   * @return {@code FilesetPO} object with version initialized
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static FilesetPO initializeFilesetPOWithVersion(
      FilesetEntity filesetEntity, FilesetPO.Builder builder) {
    try {
      String props = JsonUtils.anyFieldMapper().writeValueAsString(filesetEntity.properties());
      List<FilesetVersionPO> filesetVersionPOs =
          filesetEntity.storageLocations().entrySet().stream()
              .map(
                  entry ->
                      FilesetVersionPO.builder()
                          .withMetalakeId(builder.getFilesetMetalakeId())
                          .withCatalogId(builder.getFilesetCatalogId())
                          .withSchemaId(builder.getFilesetSchemaId())
                          .withFilesetId(filesetEntity.id())
                          .withVersion(INIT_VERSION)
                          .withFilesetComment(filesetEntity.comment())
                          .withLocationName(entry.getKey())
                          .withStorageLocation(entry.getValue())
                          .withProperties(props)
                          .withDeletedAt(DEFAULT_DELETED_AT)
                          .build())
              .collect(Collectors.toList());
      return builder
          .withFilesetId(filesetEntity.id())
          .withFilesetName(filesetEntity.name())
          .withType(filesetEntity.filesetType().name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(filesetEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .withFilesetVersionPOs(filesetVersionPOs)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update FilesetPO version
   *
   * @param oldFilesetPO the existing {@link FilesetPO} containing the current and last version data
   * @param newFileset the {@link FilesetEntity} with updated metadata and storage locations
   * @param needUpdateVersion true to increment and update version fields; false to keep versions
   *     unchanged
   * @return {@code FilesetPO} object with updated version
   * @throws RuntimeException if JSON serialization of properties fails
   */
  public static FilesetPO updateFilesetPOWithVersion(
      FilesetPO oldFilesetPO, FilesetEntity newFileset, boolean needUpdateVersion) {
    try {
      Long lastVersion = oldFilesetPO.getLastVersion();
      Long currentVersion;
      List<FilesetVersionPO> newFilesetVersionPOs;
      // Will set the version to the last version + 1
      if (needUpdateVersion) {
        lastVersion++;
        currentVersion = lastVersion;
        String props = JsonUtils.anyFieldMapper().writeValueAsString(newFileset.properties());
        newFilesetVersionPOs =
            newFileset.storageLocations().entrySet().stream()
                .map(
                    entry ->
                        FilesetVersionPO.builder()
                            .withMetalakeId(oldFilesetPO.getMetalakeId())
                            .withCatalogId(oldFilesetPO.getCatalogId())
                            .withSchemaId(oldFilesetPO.getSchemaId())
                            .withFilesetId(newFileset.id())
                            .withVersion(currentVersion)
                            .withFilesetComment(newFileset.comment())
                            .withLocationName(entry.getKey())
                            .withStorageLocation(entry.getValue())
                            .withProperties(props)
                            .withDeletedAt(DEFAULT_DELETED_AT)
                            .build())
                .collect(Collectors.toList());
      } else {
        currentVersion = oldFilesetPO.getCurrentVersion();
        newFilesetVersionPOs = oldFilesetPO.getFilesetVersionPOs();
      }
      return FilesetPO.builder()
          .withFilesetId(newFileset.id())
          .withFilesetName(newFileset.name())
          .withMetalakeId(oldFilesetPO.getMetalakeId())
          .withCatalogId(oldFilesetPO.getCatalogId())
          .withSchemaId(oldFilesetPO.getSchemaId())
          .withType(newFileset.filesetType().name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newFileset.auditInfo()))
          .withCurrentVersion(currentVersion)
          .withLastVersion(lastVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .withFilesetVersionPOs(newFilesetVersionPOs)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static boolean checkFilesetVersionNeedUpdate(
      List<FilesetVersionPO> oldFilesetVersionPOs, FilesetEntity newFileset) {
    Map<String, String> storageLocations =
        oldFilesetVersionPOs.stream()
            .collect(
                Collectors.toMap(
                    FilesetVersionPO::getLocationName, FilesetVersionPO::getStorageLocation));
    if (!StringUtils.equals(oldFilesetVersionPOs.get(0).getFilesetComment(), newFileset.comment())
        || !Objects.equals(storageLocations, newFileset.storageLocations())) {
      return true;
    }

    try {
      Map<String, String> oldProperties =
          JsonUtils.anyFieldMapper()
              .readValue(oldFilesetVersionPOs.get(0).getProperties(), Map.class);
      if (oldProperties == null) {
        return newFileset.properties() != null;
      }
      return !oldProperties.equals(newFileset.properties());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert {@link FilesetPO} to {@link FilesetEntity}
   *
   * @param filesetPO FilesetPO object to be converted
   * @param namespace Namespace object to be associated with the fileset
   * @return FilesetEntity object from FilesetPO object
   */
  public static FilesetEntity fromFilesetPO(FilesetPO filesetPO, Namespace namespace) {
    Map<String, String> storageLocations =
        filesetPO.getFilesetVersionPOs().stream()
            .collect(
                Collectors.toMap(
                    FilesetVersionPO::getLocationName, FilesetVersionPO::getStorageLocation));
    try {
      return FilesetEntity.builder()
          .withId(filesetPO.getFilesetId())
          .withName(filesetPO.getFilesetName())
          .withNamespace(namespace)
          .withComment(filesetPO.getFilesetVersionPOs().get(0).getFilesetComment())
          .withFilesetType(Fileset.Type.valueOf(filesetPO.getType()))
          .withStorageLocations(storageLocations)
          .withProperties(
              JsonUtils.anyFieldMapper()
                  .readValue(filesetPO.getFilesetVersionPOs().get(0).getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(filesetPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static TopicEntity fromTopicPO(TopicPO topicPO, Namespace namespace) {
    try {
      return TopicEntity.builder()
          .withId(topicPO.getTopicId())
          .withName(topicPO.getTopicName())
          .withNamespace(namespace)
          .withComment(topicPO.getComment())
          .withProperties(JsonUtils.anyFieldMapper().readValue(topicPO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(topicPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link FilesetPO} to list of {@link FilesetEntity}
   *
   * @param filesetPOs list of FilesetPO objects
   * @param namespace Namespace object to be associated with the fileset
   * @return list of FilesetEntity objects from list of FilesetPO objects
   */
  public static List<FilesetEntity> fromFilesetPOs(
      List<FilesetPO> filesetPOs, Namespace namespace) {
    return filesetPOs.stream()
        .map(filesetPO -> POConverters.fromFilesetPO(filesetPO, namespace))
        .collect(Collectors.toList());
  }

  public static List<TopicEntity> fromTopicPOs(List<TopicPO> topicPOs, Namespace namespace) {
    return topicPOs.stream()
        .map(topicPO -> POConverters.fromTopicPO(topicPO, namespace))
        .collect(Collectors.toList());
  }

  public static TopicPO initializeTopicPOWithVersion(
      TopicEntity topicEntity, TopicPO.Builder builder) {
    try {
      return builder
          .withTopicId(topicEntity.id())
          .withTopicName(topicEntity.name())
          .withComment(topicEntity.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(topicEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(topicEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static TopicPO updateTopicPOWithVersion(TopicPO oldTopicPO, TopicEntity newEntity) {
    Long lastVersion = oldTopicPO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return TopicPO.builder()
          .withTopicId(oldTopicPO.getTopicId())
          .withTopicName(newEntity.name())
          .withMetalakeId(oldTopicPO.getMetalakeId())
          .withCatalogId(oldTopicPO.getCatalogId())
          .withSchemaId(oldTopicPO.getSchemaId())
          .withComment(newEntity.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newEntity.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Initialize UserPO
   *
   * @param userEntity the {@link UserEntity} containing source data for the PO object
   * @param builder the {@link UserPO.Builder} used to assemble the {@link UserPO}
   * @return {@code UserPO} object with version initialized
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static UserPO initializeUserPOWithVersion(UserEntity userEntity, UserPO.Builder builder) {
    try {
      return builder
          .withUserId(userEntity.id())
          .withUserName(userEntity.name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(userEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update UserPO version
   *
   * @param oldUserPO the old UserPO object
   * @param newUser the new TableEntity object
   * @return UserPO object with updated version
   */
  public static UserPO updateUserPOWithVersion(UserPO oldUserPO, UserEntity newUser) {
    Long lastVersion = oldUserPO.getLastVersion();
    // TODO: set the version to the last version + 1 when having some fields need be multiple
    // version
    Long nextVersion = lastVersion;
    try {
      return UserPO.builder()
          .withUserId(oldUserPO.getUserId())
          .withUserName(newUser.name())
          .withMetalakeId(oldUserPO.getMetalakeId())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newUser.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link UserPO} to {@link UserEntity}
   *
   * @param userPO UserPo object to be converted
   * @param rolePOs list of rolePO
   * @param namespace Namespace object to be associated with the user
   * @return UserEntity object from UserPO object
   */
  public static UserEntity fromUserPO(UserPO userPO, List<RolePO> rolePOs, Namespace namespace) {
    try {
      List<String> roleNames =
          rolePOs.stream().map(RolePO::getRoleName).collect(Collectors.toList());
      List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

      UserEntity.Builder builder =
          UserEntity.builder()
              .withId(userPO.getUserId())
              .withName(userPO.getUserName())
              .withNamespace(namespace)
              .withAuditInfo(
                  JsonUtils.anyFieldMapper().readValue(userPO.getAuditInfo(), AuditInfo.class));
      if (!roleNames.isEmpty()) {
        builder.withRoleNames(roleNames);
      }
      if (!roleIds.isEmpty()) {
        builder.withRoleIds(roleIds);
      }
      return builder.build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert {@link ExtendedUserPO} to {@link UserEntity}
   *
   * @param userPO ExtendedUserPO object to be converted
   * @param namespace Namespace object to be associated with the user
   * @return UserEntity object from ExtendedUserPO object
   */
  public static UserEntity fromExtendedUserPO(ExtendedUserPO userPO, Namespace namespace) {
    try {
      UserEntity.Builder builder =
          UserEntity.builder()
              .withId(userPO.getUserId())
              .withName(userPO.getUserName())
              .withNamespace(namespace)
              .withAuditInfo(
                  JsonUtils.anyFieldMapper().readValue(userPO.getAuditInfo(), AuditInfo.class));
      if (StringUtils.isNotBlank(userPO.getRoleNames())) {
        List<String> roleNamesFromJson =
            JsonUtils.anyFieldMapper().readValue(userPO.getRoleNames(), List.class);
        List<String> roleNames =
            roleNamesFromJson.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
        if (!roleNames.isEmpty()) {
          builder.withRoleNames(roleNames);
        }
      }

      if (StringUtils.isNotBlank(userPO.getRoleIds())) {
        // Different JSON AGG from backends will produce different types data, we
        // can only use Object. PostSQL produces the data with type Long. H2 produces
        // the data with type String.
        List<Object> roleIdsFromJson =
            JsonUtils.anyFieldMapper().readValue(userPO.getRoleIds(), List.class);
        List<Long> roleIds =
            roleIdsFromJson.stream()
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .filter(StringUtils::isNotBlank)
                .map(Long::valueOf)
                .collect(Collectors.toList());

        if (!roleIds.isEmpty()) {
          builder.withRoleIds(roleIds);
        }
      }
      return builder.build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert {@link GroupPO} to {@link GroupEntity}
   *
   * @param groupPO GroupPO object to be converted
   * @param rolePOs list of rolePO
   * @param namespace Namespace object to be associated with the group
   * @return GroupEntity object from GroupPO object
   */
  public static GroupEntity fromGroupPO(
      GroupPO groupPO, List<RolePO> rolePOs, Namespace namespace) {
    try {
      List<String> roleNames =
          rolePOs.stream().map(RolePO::getRoleName).collect(Collectors.toList());
      List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

      GroupEntity.Builder builder =
          GroupEntity.builder()
              .withId(groupPO.getGroupId())
              .withName(groupPO.getGroupName())
              .withNamespace(namespace)
              .withAuditInfo(
                  JsonUtils.anyFieldMapper().readValue(groupPO.getAuditInfo(), AuditInfo.class));
      if (!roleNames.isEmpty()) {
        builder.withRoleNames(roleNames);
      }
      if (!roleIds.isEmpty()) {
        builder.withRoleIds(roleIds);
      }
      return builder.build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert {@link ExtendedGroupPO} to {@link GroupEntity}
   *
   * @param groupPO ExtendedGroupPO object to be converted
   * @param namespace Namespace object to be associated with the user
   * @return GroupEntity object from ExtendedGroupPO object
   */
  public static GroupEntity fromExtendedGroupPO(ExtendedGroupPO groupPO, Namespace namespace) {
    try {
      GroupEntity.Builder builder =
          GroupEntity.builder()
              .withId(groupPO.getGroupId())
              .withName(groupPO.getGroupName())
              .withNamespace(namespace)
              .withAuditInfo(
                  JsonUtils.anyFieldMapper().readValue(groupPO.getAuditInfo(), AuditInfo.class));

      if (StringUtils.isNotBlank(groupPO.getRoleNames())) {
        List<String> roleNamesFromJson =
            JsonUtils.anyFieldMapper().readValue(groupPO.getRoleNames(), List.class);
        List<String> roleNames =
            roleNamesFromJson.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
        if (!roleNames.isEmpty()) {
          builder.withRoleNames(roleNames);
        }
      }

      if (StringUtils.isNotBlank(groupPO.getRoleIds())) {
        // Different JSON AGG from backends will produce different types data, we
        // can only use Object. PostSQL produces the data with type Long. H2 produces
        // the data with type String.
        List<Object> roleIdsFromJson =
            JsonUtils.anyFieldMapper().readValue(groupPO.getRoleIds(), List.class);
        List<Long> roleIds =
            roleIdsFromJson.stream()
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .filter(StringUtils::isNotBlank)
                .map(Long::valueOf)
                .collect(Collectors.toList());

        if (!roleIds.isEmpty()) {
          builder.withRoleIds(roleIds);
        }
      }
      return builder.build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Initialize UserRoleRelPO
   *
   * @param userEntity UserEntity object
   * @param roleIds list of role ids
   * @return UserRoleRelPO object with version initialized
   */
  public static List<UserRoleRelPO> initializeUserRoleRelsPOWithVersion(
      UserEntity userEntity, List<Long> roleIds) {
    try {
      List<UserRoleRelPO> userRoleRelPOs = Lists.newArrayList();
      for (Long roleId : roleIds) {
        UserRoleRelPO roleRelPO =
            UserRoleRelPO.builder()
                .withUserId(userEntity.id())
                .withRoleId(roleId)
                .withAuditInfo(
                    JsonUtils.anyFieldMapper().writeValueAsString(userEntity.auditInfo()))
                .withCurrentVersion(INIT_VERSION)
                .withLastVersion(INIT_VERSION)
                .withDeletedAt(DEFAULT_DELETED_AT)
                .build();
        userRoleRelPOs.add(roleRelPO);
      }
      return userRoleRelPOs;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Initialize RolePO
   *
   * @param roleEntity the {@link RoleEntity} containing source data for the PO object
   * @param builder the {@link RolePO.Builder} used to assemble the {@link RolePO}
   * @return {@code RolePO} object with version initialized
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static RolePO initializeRolePOWithVersion(RoleEntity roleEntity, RolePO.Builder builder) {
    try {
      return builder
          .withRoleId(roleEntity.id())
          .withRoleName(roleEntity.name())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(roleEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(roleEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Initialize GroupPO
   *
   * @param groupEntity the {@link GroupEntity} containing source data for the PO object
   * @param builder the {@link GroupPO.Builder} used to assemble the {@link GroupPO}
   * @return {@code GroupPO} object with version initialized
   * @throws RuntimeException if JSON serialization of the entity fails
   */
  public static GroupPO initializeGroupPOWithVersion(
      GroupEntity groupEntity, GroupPO.Builder builder) {
    try {
      return builder
          .withGroupId(groupEntity.id())
          .withGroupName(groupEntity.name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(groupEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update GroupPO version
   *
   * @param oldGroupPO the old GroupPO object
   * @param newGroup the new GroupEntity object
   * @return GroupPO object with updated version
   */
  public static GroupPO updateGroupPOWithVersion(GroupPO oldGroupPO, GroupEntity newGroup) {
    Long lastVersion = oldGroupPO.getLastVersion();
    // TODO: set the version to the last version + 1 when having some fields need be multiple
    // version
    Long nextVersion = lastVersion;
    try {
      return GroupPO.builder()
          .withGroupId(oldGroupPO.getGroupId())
          .withGroupName(newGroup.name())
          .withMetalakeId(oldGroupPO.getMetalakeId())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newGroup.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Initialize GroupRoleRelPO
   *
   * @param groupEntity GroupEntity object
   * @param roleIds list of role ids
   * @return GroupRoleRelPO object with version initialized
   */
  public static List<GroupRoleRelPO> initializeGroupRoleRelsPOWithVersion(
      GroupEntity groupEntity, List<Long> roleIds) {
    try {
      List<GroupRoleRelPO> groupRoleRelPOS = Lists.newArrayList();
      for (Long roleId : roleIds) {
        GroupRoleRelPO roleRelPO =
            GroupRoleRelPO.builder()
                .withGroupId(groupEntity.id())
                .withRoleId(roleId)
                .withAuditInfo(
                    JsonUtils.anyFieldMapper().writeValueAsString(groupEntity.auditInfo()))
                .withCurrentVersion(INIT_VERSION)
                .withLastVersion(INIT_VERSION)
                .withDeletedAt(DEFAULT_DELETED_AT)
                .build();
        groupRoleRelPOS.add(roleRelPO);
      }
      return groupRoleRelPOS;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static SecurableObject fromSecurableObjectPO(
      String fullName, SecurableObjectPO securableObjectPO, MetadataObject.Type type) {
    try {
      List<String> privilegeNames =
          JsonUtils.anyFieldMapper().readValue(securableObjectPO.getPrivilegeNames(), List.class);
      List<String> privilegeConditions =
          JsonUtils.anyFieldMapper()
              .readValue(securableObjectPO.getPrivilegeConditions(), List.class);

      List<Privilege> privileges = Lists.newArrayList();
      for (int index = 0; index < privilegeNames.size(); index++) {
        if (Privilege.Condition.ALLOW.name().equals(privilegeConditions.get(index))) {
          privileges.add(Privileges.allow(privilegeNames.get(index)));
        } else {
          privileges.add(Privileges.deny(privilegeNames.get(index)));
        }
      }

      return SecurableObjects.parse(fullName, type, privileges);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static RoleEntity fromRolePO(
      RolePO rolePO, List<SecurableObject> securableObjects, Namespace namespace) {
    try {
      return RoleEntity.builder()
          .withId(rolePO.getRoleId())
          .withName(rolePO.getRoleName())
          .withNamespace(namespace)
          .withProperties(JsonUtils.anyFieldMapper().readValue(rolePO.getProperties(), Map.class))
          .withSecurableObjects(securableObjects)
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(rolePO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static SecurableObjectPO.Builder initializeSecurablePOBuilderWithVersion(
      long roleId, SecurableObject securableObject, String type) {
    try {
      SecurableObjectPO.Builder builder = SecurableObjectPO.builder();
      builder
          .withRoleId(roleId)
          .withType(type)
          .withPrivilegeConditions(
              JsonUtils.anyFieldMapper()
                  .writeValueAsString(
                      securableObject.privileges().stream()
                          .map(Privilege::condition)
                          .map(Privilege.Condition::name)
                          .collect(Collectors.toList())))
          .withPrivilegeNames(
              JsonUtils.anyFieldMapper()
                  .writeValueAsString(
                      securableObject.privileges().stream()
                          .map(Privilege::name)
                          .map(Privilege.Name::name)
                          .collect(Collectors.toList())))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT);

      return builder;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static RolePO updateRolePOWithVersion(RolePO oldRolePO, RoleEntity newRole) {
    Long lastVersion = oldRolePO.getLastVersion();
    // TODO: set the version to the last version + 1 when having some fields need be multiple
    // version
    Long nextVersion = lastVersion;
    try {
      return RolePO.builder()
          .withRoleId(oldRolePO.getRoleId())
          .withRoleName(newRole.name())
          .withMetalakeId(oldRolePO.getMetalakeId())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newRole.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newRole.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static TagEntity fromTagPO(TagPO tagPO, Namespace namespace) {
    try {
      return TagEntity.builder()
          .withId(tagPO.getTagId())
          .withName(tagPO.getTagName())
          .withNamespace(namespace)
          .withComment(tagPO.getComment())
          .withProperties(JsonUtils.anyFieldMapper().readValue(tagPO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(tagPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static TagPO initializeTagPOWithVersion(TagEntity tagEntity, TagPO.Builder builder) {
    try {
      return builder
          .withTagId(tagEntity.id())
          .withTagName(tagEntity.name())
          .withComment(tagEntity.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(tagEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(tagEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static TagPO updateTagPOWithVersion(TagPO oldTagPO, TagEntity newEntity) {
    Long lastVersion = oldTagPO.getLastVersion();
    // TODO: set the version to the last version + 1 when having some fields need be multiple
    // version
    Long nextVersion = lastVersion;
    try {
      return TagPO.builder()
          .withTagId(oldTagPO.getTagId())
          .withTagName(newEntity.name())
          .withMetalakeId(oldTagPO.getMetalakeId())
          .withComment(newEntity.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newEntity.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static TagMetadataObjectRelPO initializeTagMetadataObjectRelPOWithVersion(
      Long tagId, Long metadataObjectId, String metadataObjectType) {
    try {
      AuditInfo auditInfo =
          AuditInfo.builder()
              .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
              .withCreateTime(Instant.now())
              .build();

      return TagMetadataObjectRelPO.builder()
          .withTagId(tagId)
          .withMetadataObjectId(metadataObjectId)
          .withMetadataObjectType(metadataObjectType)
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static OwnerRelPO initializeOwnerRelPOsWithVersion(
      Long metalakeId,
      String ownerType,
      Long ownerId,
      String metadataObjectType,
      Long metadataObjectId) {
    try {
      AuditInfo auditInfo =
          AuditInfo.builder()
              .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
              .withCreateTime(Instant.now())
              .build();
      return OwnerRelPO.builder()
          .withMetalakeId(metalakeId)
          .withOwnerId(ownerId)
          .withOwnerType(ownerType)
          .withMetadataObjectId(metadataObjectId)
          .withMetadataObjectType(metadataObjectType)
          .withAuditIfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeleteAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static ModelEntity fromModelPO(ModelPO modelPO, Namespace namespace) {
    try {
      return ModelEntity.builder()
          .withId(modelPO.getModelId())
          .withName(modelPO.getModelName())
          .withNamespace(namespace)
          .withComment(modelPO.getModelComment())
          .withLatestVersion(modelPO.getModelLatestVersion())
          .withProperties(
              JsonUtils.anyFieldMapper().readValue(modelPO.getModelProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(modelPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static ModelPO initializeModelPO(ModelEntity modelEntity, ModelPO.Builder builder) {
    try {
      return builder
          .withModelId(modelEntity.id())
          .withModelName(modelEntity.name())
          .withModelComment(modelEntity.comment())
          .withModelLatestVersion(modelEntity.latestVersion())
          .withModelProperties(
              JsonUtils.anyFieldMapper().writeValueAsString(modelEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(modelEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static ModelVersionEntity fromModelVersionPO(
      NameIdentifier modelIdent,
      ModelVersionPO modelVersionPO,
      List<ModelVersionAliasRelPO> aliasRelPOs) {
    List<String> aliases =
        aliasRelPOs.stream()
            .map(ModelVersionAliasRelPO::getModelVersionAlias)
            .collect(Collectors.toList());

    try {
      return ModelVersionEntity.builder()
          .withModelIdentifier(modelIdent)
          .withVersion(modelVersionPO.getModelVersion())
          .withAliases(aliases)
          .withComment(modelVersionPO.getModelVersionComment())
          .withUri(modelVersionPO.getModelVersionUri())
          .withProperties(
              JsonUtils.anyFieldMapper()
                  .readValue(modelVersionPO.getModelVersionProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(modelVersionPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Updata ModelPO with new ModelEntity object, metalakeID, catalogID, schemaID will be the same as
   * the old one. the id, name, comment, properties, latestVersion and auditInfo will be updated.
   *
   * @param oldModelPO the old ModelPO object
   * @param newModel the new ModelEntity object
   * @return the updated ModelPO object
   */
  public static ModelPO updateModelPO(ModelPO oldModelPO, ModelEntity newModel) {
    try {
      return ModelPO.builder()
          .withModelId(newModel.id())
          .withModelName(newModel.name())
          .withMetalakeId(oldModelPO.getMetalakeId())
          .withCatalogId(oldModelPO.getCatalogId())
          .withSchemaId(oldModelPO.getSchemaId())
          .withModelComment(newModel.comment())
          .withModelLatestVersion(newModel.latestVersion())
          .withModelProperties(JsonUtils.anyFieldMapper().writeValueAsString(newModel.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newModel.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update ModelVersionPO with new ModelVersionEntity object, metalakeID, catalogID, schemaID and
   * modelID will be the same as the old one. uri, comment, properties, version and auditInfo will
   * be updated.
   *
   * @param oldModelVersionPO the old ModelVersionPO object
   * @param newModelVersion the new ModelVersionEntity object
   * @return the updated ModelVersionPO object
   */
  public static ModelVersionPO updateModelVersionPO(
      ModelVersionPO oldModelVersionPO, ModelVersionEntity newModelVersion) {
    try {
      return ModelVersionPO.builder()
          .withModelId(oldModelVersionPO.getModelId())
          .withMetalakeId(oldModelVersionPO.getMetalakeId())
          .withCatalogId(oldModelVersionPO.getCatalogId())
          .withSchemaId(oldModelVersionPO.getSchemaId())
          .withModelVersionUri(newModelVersion.uri())
          .withModelVersion(oldModelVersionPO.getModelVersion())
          .withModelVersionComment(newModelVersion.comment())
          .withModelVersionProperties(
              JsonUtils.anyFieldMapper().writeValueAsString(newModelVersion.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newModelVersion.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Construct a new ModelVersionAliasRelPO object with the given alias.
   *
   * @param oldModelVersionAliasRelPOs The old ModelVersionAliasRelPOs object
   * @param newModelVersion The new {@link ModelVersionEntity} object
   * @return The new ModelVersionAliasRelPO object
   */
  public static List<ModelVersionAliasRelPO> updateModelVersionAliasRelPO(
      List<ModelVersionAliasRelPO> oldModelVersionAliasRelPOs, ModelVersionEntity newModelVersion) {

    if (!oldModelVersionAliasRelPOs.isEmpty()) {
      ModelVersionAliasRelPO oldModelVersionAliasRelPO = oldModelVersionAliasRelPOs.get(0);
      return newModelVersion.aliases().stream()
          .map(alias -> createAliasRelPO(oldModelVersionAliasRelPO, alias))
          .collect(Collectors.toList());
    } else {
      return newModelVersion.aliases().stream()
          .map(alias -> createAliasRelPO(newModelVersion, alias))
          .collect(Collectors.toList());
    }
  }

  public static ModelVersionPO initializeModelVersionPO(
      ModelVersionEntity modelVersionEntity, ModelVersionPO.Builder builder) {
    try {
      return builder
          // Note that version set here will not be used when inserting into database, it will
          // directly use the version from the query to avoid concurrent version conflict.
          .withModelVersion(modelVersionEntity.version())
          .withModelVersionComment(modelVersionEntity.comment())
          .withModelVersionUri(modelVersionEntity.uri())
          .withModelVersionProperties(
              JsonUtils.anyFieldMapper().writeValueAsString(modelVersionEntity.properties()))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().writeValueAsString(modelVersionEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static List<ModelVersionAliasRelPO> initializeModelVersionAliasRelPO(
      ModelVersionEntity modelVersionEntity, Long modelId) {
    return modelVersionEntity.aliases().stream()
        .map(
            a ->
                ModelVersionAliasRelPO.builder()
                    // Note that version set here will not be used when inserting into database, it
                    // will directly use the version from the query to avoid concurrent version
                    // conflict.
                    .withModelVersion(modelVersionEntity.version())
                    .withModelVersionAlias(a)
                    .withModelId(modelId)
                    .withDeletedAt(DEFAULT_DELETED_AT)
                    .build())
        .collect(Collectors.toList());
  }

  private static ModelVersionAliasRelPO createAliasRelPO(
      ModelVersionAliasRelPO oldModelVersionAliasRelPO, String alias) {
    return ModelVersionAliasRelPO.builder()
        .withModelVersion(oldModelVersionAliasRelPO.getModelVersion())
        .withModelVersionAlias(alias)
        .withModelId(oldModelVersionAliasRelPO.getModelId())
        .withDeletedAt(DEFAULT_DELETED_AT)
        .build();
  }

  private static ModelVersionAliasRelPO createAliasRelPO(ModelVersionEntity entity, String alias) {
    return ModelVersionAliasRelPO.builder()
        .withModelVersion(entity.version())
        .withModelVersionAlias(alias)
        .withModelId(entity.id())
        .withDeletedAt(DEFAULT_DELETED_AT)
        .build();
  }
}
