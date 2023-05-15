package com.datastrato.unified_catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class Zone implements Entity, Auditable {

  private static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the zone");
  private static final Field LAKEHOUSE_ID =
      Field.required("lakehouse_id", Long.class, "The unique identifier of the lakehouse");
  private static final Field NAME = Field.required("name", String.class, "The name of the zone");
  private static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the zone");
  private static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the zone");
  private static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the zone");

  @JsonProperty("id")
  private Long id;

  @JsonProperty("lakehouse_id")
  private Long lakehouseId;

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  private Zone() {}

  @Override
  public Map<Field, Object> fields() {
    return new ImmutableMap.Builder<Field, Object>()
        .put(ID, id)
        .put(LAKEHOUSE_ID, lakehouseId)
        .put(NAME, name)
        .put(COMMENT, comment)
        .put(PROPERTIES, properties)
        .put(AUDIT_INFO, auditInfo)
        .build();
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  public static class Builder {
    private final Zone zone;

    public Builder() {
      zone = new Zone();
    }

    public Builder id(Long id) {
      zone.id = id;
      return this;
    }

    public Builder lakehouseId(Long lakehouseId) {
      zone.lakehouseId = lakehouseId;
      return this;
    }

    public Builder name(String name) {
      zone.name = name;
      return this;
    }

    public Builder comment(String comment) {
      zone.comment = comment;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      zone.properties = properties;
      return this;
    }

    public Builder auditInfo(AuditInfo auditInfo) {
      zone.auditInfo = auditInfo;
      return this;
    }

    public Zone build() {
      zone.validate();
      return zone;
    }
  }
}
