package com.datastrato.unified_catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.Type;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public final class Column implements Entity, Auditable {

  private static final Field ID =
      Field.required("id", Integer.class, "The unique identifier of the column");
  private static final Field ENTITY_ID =
      Field.required(
          "entity_id",
          Long.class,
          "The unique identifier of the parent entity which this column belongs to");
  private static final Field ENTITY_SNAPSHOT_ID =
      Field.required(
          "entity_snapshot_id",
          Long.class,
          "The snapshot id of the parent entity which this column belongs to");
  private static final Field NAME = Field.required("name", String.class, "The name of the column");
  private static final Field TYPE = Field.required("type", Type.class, "The type of the column");
  private static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the column");
  private static final Field POSITION =
      Field.required("position", Integer.class, "The position of the column");
  private static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the column");

  @JsonProperty("id")
  private Integer id;

  @JsonProperty("entity_id")
  private Long entityId;

  @JsonProperty("entity_snapshot_id")
  private Long entitySnapshotId;

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private Type type;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("position")
  private Integer position;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  private Column() {}

  @Override
  public Map<Field, Object> fields() {
    return new ImmutableMap.Builder<Field, Object>()
        .put(ID, id)
        .put(ENTITY_ID, entityId)
        .put(ENTITY_SNAPSHOT_ID, entitySnapshotId)
        .put(NAME, name)
        .put(TYPE, type)
        .put(COMMENT, comment)
        .put(POSITION, position)
        .build();
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  public static class Builder {
    private final Column column;

    public Builder() {
      column = new Column();
    }

    public Builder id(Integer id) {
      column.id = id;
      return this;
    }

    public Builder entityId(Long entityId) {
      column.entityId = entityId;
      return this;
    }

    public Builder entitySnapshotId(Long entitySnapshotId) {
      column.entitySnapshotId = entitySnapshotId;
      return this;
    }

    public Builder name(String name) {
      column.name = name;
      return this;
    }

    public Builder type(Type type) {
      column.type = type;
      return this;
    }

    public Builder comment(String comment) {
      column.comment = comment;
      return this;
    }

    public Builder position(Integer position) {
      column.position = position;
      return this;
    }

    public Builder auditInfo(AuditInfo auditInfo) {
      column.auditInfo = auditInfo;
      return this;
    }

    public Column build() {
      column.validate();
      return column;
    }
  }
}
