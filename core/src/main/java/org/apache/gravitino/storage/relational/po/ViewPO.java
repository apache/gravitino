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
package org.apache.gravitino.storage.relational.po;

import static org.apache.gravitino.storage.relational.utils.POConverters.DEFAULT_DELETED_AT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.storage.relational.service.EntityIdService;

@Getter
@EqualsAndHashCode(exclude = "viewVersionInfoPO")
@ToString
public class ViewPO {

  public static final Long INITIAL_VERSION = 1L;

  private Long viewId;
  private String viewName;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;
  private ViewVersionInfoPO viewVersionInfoPO;

  public ViewPO() {}

  public static class ViewPOBuilder {
    // Lombok will generate builder methods.
  }

  @lombok.Builder(setterPrefix = "with")
  private ViewPO(
      Long viewId,
      String viewName,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      String auditInfo,
      Long currentVersion,
      Long lastVersion,
      Long deletedAt,
      ViewVersionInfoPO viewVersionInfoPO) {
    Preconditions.checkArgument(viewId != null, "View id is required");
    Preconditions.checkArgument(viewName != null, "View name is required");
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(auditInfo != null, "Audit info is required");
    Preconditions.checkArgument(currentVersion != null, "Current version is required");
    Preconditions.checkArgument(lastVersion != null, "Last version is required");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.viewId = viewId;
    this.viewName = viewName;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.auditInfo = auditInfo;
    this.currentVersion = currentVersion;
    this.lastVersion = lastVersion;
    this.deletedAt = deletedAt;
    this.viewVersionInfoPO = viewVersionInfoPO;
  }

  // ============================ PO Converters ============================

  public static ViewEntity fromViewPO(ViewPO viewPO, Namespace namespace) {
    try {
      ViewVersionInfoPO versionPO = viewPO.getViewVersionInfoPO();
      List<ColumnDTO> columnDTOs =
          JsonUtils.anyFieldMapper()
              .readValue(
                  versionPO.columns(),
                  JsonUtils.anyFieldMapper()
                      .getTypeFactory()
                      .constructCollectionType(List.class, ColumnDTO.class));
      Column[] columns = columnDTOs.toArray(new Column[0]);

      Representation[] representations = deserializeRepresentations(versionPO.representations());

      Map<String, String> properties =
          versionPO.properties() == null
              ? Collections.emptyMap()
              : JsonUtils.anyFieldMapper()
                  .readValue(
                      versionPO.properties(),
                      JsonUtils.anyFieldMapper()
                          .getTypeFactory()
                          .constructMapType(Map.class, String.class, String.class));

      return ViewEntity.builder()
          .withId(viewPO.getViewId())
          .withName(viewPO.getViewName())
          .withNamespace(namespace)
          .withComment(versionPO.viewComment())
          .withColumns(columns)
          .withRepresentations(representations)
          .withDefaultCatalog(versionPO.defaultCatalog())
          .withDefaultSchema(versionPO.defaultSchema())
          .withProperties(properties)
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(viewPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static ViewPO initializeViewPO(ViewEntity viewEntity, ViewPOBuilder builder) {
    builder.withCurrentVersion(INITIAL_VERSION).withLastVersion(INITIAL_VERSION);
    return buildViewPO(viewEntity, builder, INITIAL_VERSION.intValue());
  }

  public static ViewVersionInfoPO initializeViewVersionInfoPO(
      ViewEntity viewEntity, NamespacedEntityId namespacedEntityId, Integer version) {
    try {
      List<ColumnDTO> columnDTOs =
          Arrays.stream(viewEntity.columns() == null ? new Column[0] : viewEntity.columns())
              .map(ViewPO::toColumnDTO)
              .collect(Collectors.toList());
      String propertiesJson =
          viewEntity.properties() == null || viewEntity.properties().isEmpty()
              ? null
              : JsonUtils.anyFieldMapper().writeValueAsString(viewEntity.properties());

      return ViewVersionInfoPO.builder()
          .withViewId(viewEntity.id())
          .withMetalakeId(namespacedEntityId.namespaceIds()[0])
          .withCatalogId(namespacedEntityId.namespaceIds()[1])
          .withSchemaId(namespacedEntityId.entityId())
          .withVersion(version)
          .withViewComment(viewEntity.comment())
          .withColumns(JsonUtils.anyFieldMapper().writeValueAsString(columnDTOs))
          .withProperties(propertiesJson)
          .withDefaultCatalog(viewEntity.defaultCatalog())
          .withDefaultSchema(viewEntity.defaultSchema())
          .withRepresentations(serializeRepresentations(viewEntity.representations()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(viewEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static ViewPO buildViewPO(ViewEntity viewEntity, ViewPOBuilder builder, Integer version) {
    try {
      NamespacedEntityId namespacedEntityId =
          EntityIdService.getEntityIds(
              NameIdentifier.of(viewEntity.namespace().levels()), Entity.EntityType.SCHEMA);
      ViewVersionInfoPO versionPO =
          initializeViewVersionInfoPO(viewEntity, namespacedEntityId, version);
      return builder
          .withViewId(viewEntity.id())
          .withViewName(viewEntity.name())
          .withMetalakeId(namespacedEntityId.namespaceIds()[0])
          .withCatalogId(namespacedEntityId.namespaceIds()[1])
          .withSchemaId(namespacedEntityId.entityId())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(viewEntity.auditInfo()))
          .withViewVersionInfoPO(versionPO)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  // ============================ Representation Serde ============================

  static String serializeRepresentations(Representation[] representations)
      throws JsonProcessingException {
    List<RepresentationDTO> out =
        Arrays.stream(representations == null ? new Representation[0] : representations)
            .map(RepresentationDTO::fromRepresentation)
            .collect(Collectors.toList());
    return JsonUtils.anyFieldMapper().writeValueAsString(out);
  }

  static Representation[] deserializeRepresentations(String json) throws JsonProcessingException {
    CollectionType type =
        JsonUtils.anyFieldMapper()
            .getTypeFactory()
            .constructCollectionType(List.class, RepresentationDTO.class);
    List<RepresentationDTO> list = JsonUtils.anyFieldMapper().readValue(json, type);
    return list.stream().map(RepresentationDTO::toRepresentation).toArray(Representation[]::new);
  }

  private static ColumnDTO toColumnDTO(Column column) {
    if (column instanceof ColumnDTO) {
      return (ColumnDTO) column;
    }
    return ColumnDTO.builder()
        .withName(column.name())
        .withDataType(column.dataType())
        .withComment(column.comment())
        .withNullable(column.nullable())
        .withAutoIncrement(column.autoIncrement())
        .build();
  }
}
