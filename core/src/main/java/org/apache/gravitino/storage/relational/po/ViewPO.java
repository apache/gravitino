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
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.storage.relational.service.EntityIdService;

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

  public Long getViewId() {
    return viewId;
  }

  public String getViewName() {
    return viewName;
  }

  public Long getMetalakeId() {
    return metalakeId;
  }

  public Long getCatalogId() {
    return catalogId;
  }

  public Long getSchemaId() {
    return schemaId;
  }

  public String getAuditInfo() {
    return auditInfo;
  }

  public Long getCurrentVersion() {
    return currentVersion;
  }

  public Long getLastVersion() {
    return lastVersion;
  }

  public Long getDeletedAt() {
    return deletedAt;
  }

  public ViewVersionInfoPO getViewVersionInfoPO() {
    return viewVersionInfoPO;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ViewPO)) {
      return false;
    }
    ViewPO other = (ViewPO) o;
    return Objects.equals(viewId, other.viewId)
        && Objects.equals(viewName, other.viewName)
        && Objects.equals(metalakeId, other.metalakeId)
        && Objects.equals(catalogId, other.catalogId)
        && Objects.equals(schemaId, other.schemaId)
        && Objects.equals(auditInfo, other.auditInfo)
        && Objects.equals(currentVersion, other.currentVersion)
        && Objects.equals(lastVersion, other.lastVersion)
        && Objects.equals(deletedAt, other.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        viewId,
        viewName,
        metalakeId,
        catalogId,
        schemaId,
        auditInfo,
        currentVersion,
        lastVersion,
        deletedAt);
  }

  public static class Builder {
    private final ViewPO viewPO;

    private Builder() {
      viewPO = new ViewPO();
    }

    public Builder withViewId(Long viewId) {
      viewPO.viewId = viewId;
      return this;
    }

    public Builder withViewName(String viewName) {
      viewPO.viewName = viewName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      viewPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      viewPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      viewPO.schemaId = schemaId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      viewPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      viewPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      viewPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      viewPO.deletedAt = deletedAt;
      return this;
    }

    public Builder withViewVersionInfoPO(ViewVersionInfoPO viewVersionInfoPO) {
      viewPO.viewVersionInfoPO = viewVersionInfoPO;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(viewPO.viewId != null, "View id is required");
      Preconditions.checkArgument(viewPO.viewName != null, "View name is required");
      Preconditions.checkArgument(viewPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(viewPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(viewPO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(viewPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(viewPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(viewPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(viewPO.deletedAt != null, "Deleted at is required");
    }

    public ViewPO build() {
      validate();
      return viewPO;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
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

  public static ViewPO initializeViewPO(ViewEntity viewEntity, Builder builder) {
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

  public static ViewPO buildViewPO(ViewEntity viewEntity, Builder builder, Integer version) {
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
    List<Map<String, Object>> out =
        Arrays.stream(representations == null ? new Representation[0] : representations)
            .map(ViewPO::toMap)
            .collect(Collectors.toList());
    return JsonUtils.anyFieldMapper().writeValueAsString(out);
  }

  static Representation[] deserializeRepresentations(String json) throws JsonProcessingException {
    CollectionType type =
        JsonUtils.anyFieldMapper().getTypeFactory().constructCollectionType(List.class, Map.class);
    List<Map<String, Object>> list = JsonUtils.anyFieldMapper().readValue(json, type);
    return list.stream().map(ViewPO::fromMap).toArray(Representation[]::new);
  }

  private static Map<String, Object> toMap(Representation rep) {
    if (rep instanceof SQLRepresentation) {
      SQLRepresentation sql = (SQLRepresentation) rep;
      ImmutableMap.Builder<String, Object> m = ImmutableMap.builder();
      m.put("type", Representation.TYPE_SQL);
      m.put("dialect", sql.dialect());
      m.put("sql", sql.sql());
      return m.build();
    }
    throw new IllegalArgumentException(
        "Unsupported representation type: " + rep.getClass().getName());
  }

  private static Representation fromMap(Map<String, Object> map) {
    Object type = map.get("type");
    if (type == null || Representation.TYPE_SQL.equals(type)) {
      return SQLRepresentation.builder()
          .withDialect((String) map.get("dialect"))
          .withSql((String) map.get("sql"))
          .build();
    }
    throw new IllegalArgumentException("Unsupported representation type: " + type);
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
