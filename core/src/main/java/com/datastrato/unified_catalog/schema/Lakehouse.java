package com.datastrato.unified_catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class Lakehouse implements Entity, Auditable {

  private static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the lakehouse");
  private static final Field NAME =
      Field.required("name", String.class, "The name of the lakehouse");
  private static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the lakehouse");
  private static final Field PROPERTIES =
      Field.required("properties", Map.class, "The properties of the lakehouse");
  private static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the lakehouse");

  @JsonProperty("id")
  private Long id;

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  private Lakehouse() {}

  @Override
  public Map<Field, Object> fields() {
    return new ImmutableMap.Builder<Field, Object>()
        .put(ID, id)
        .put(NAME, name)
        .put(COMMENT, comment)
        .put(PROPERTIES, properties)
        .build();
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  public static class Builder {
    private final Lakehouse lakehouse;

    public Builder() {
      lakehouse = new Lakehouse();
    }

    public Builder id(Long id) {
      lakehouse.id = id;
      return this;
    }

    public Builder name(String name) {
      lakehouse.name = name;
      return this;
    }

    public Builder comment(String comment) {
      lakehouse.comment = comment;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      lakehouse.properties = properties;
      return this;
    }

    public Builder auditInfo(AuditInfo auditInfo) {
      lakehouse.auditInfo = auditInfo;
      return this;
    }

    public Lakehouse build() {
      lakehouse.validate();
      return lakehouse;
    }
  }
}
