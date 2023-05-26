package com.datastrato.graviton.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class Zone implements Entity, Auditable {

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the zone");
  public static final Field LAKEHOUSE_ID =
      Field.required("lakehouse_id", Long.class, "The unique identifier of the lakehouse");
  public static final Field NAME = Field.required("name", String.class, "The name of the zone");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the zone");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the zone");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the zone");

  @JsonProperty("id")
  private Long id;

  @JsonProperty("lakehouse_id")
  private Long lakehouseId;

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  private Zone() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(LAKEHOUSE_ID, lakehouseId);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
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

    public Builder withId(Long id) {
      zone.id = id;
      return this;
    }

    public Builder withLakehouseId(Long lakehouseId) {
      zone.lakehouseId = lakehouseId;
      return this;
    }

    public Builder withName(String name) {
      zone.name = name;
      return this;
    }

    public Builder withComment(String comment) {
      zone.comment = comment;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      zone.properties = properties;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      zone.auditInfo = auditInfo;
      return this;
    }

    public Zone build() {
      zone.validate();
      return zone;
    }
  }
}
