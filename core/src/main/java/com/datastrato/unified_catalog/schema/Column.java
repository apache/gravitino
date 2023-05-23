package com.datastrato.unified_catalog.schema;

import com.datastrato.unified_catalog.schema.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.substrait.type.Type;
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
public final class Column implements Entity, Auditable {

  public static final Field ID =
      Field.required("id", Integer.class, "The unique identifier of the column");
  public static final Field ENTITY_ID =
      Field.required(
          "entity_id",
          Long.class,
          "The unique identifier of the parent entity which this column belongs to");
  public static final Field ENTITY_SNAPSHOT_ID =
      Field.required(
          "entity_snapshot_id",
          Long.class,
          "The snapshot id of the parent entity which this column belongs to");
  public static final Field NAME = Field.required("name", String.class, "The name of the column");
  public static final Field TYPE = Field.required("type", Type.class, "The type of the column");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the column");
  public static final Field POSITION =
      Field.required("position", Integer.class, "The position of the column");
  public static final Field AUDIT_INFO =
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
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type type;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("position")
  private Integer position;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  private Column() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(ENTITY_ID, entityId);
    fields.put(ENTITY_SNAPSHOT_ID, entitySnapshotId);
    fields.put(NAME, name);
    fields.put(TYPE, type);
    fields.put(COMMENT, comment);
    fields.put(POSITION, position);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
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

    public Builder withId(Integer id) {
      column.id = id;
      return this;
    }

    public Builder withEntityId(Long entityId) {
      column.entityId = entityId;
      return this;
    }

    public Builder withEntitySnapshotId(Long entitySnapshotId) {
      column.entitySnapshotId = entitySnapshotId;
      return this;
    }

    public Builder withName(String name) {
      column.name = name;
      return this;
    }

    public Builder withType(Type type) {
      column.type = type;
      return this;
    }

    public Builder withComment(String comment) {
      column.comment = comment;
      return this;
    }

    public Builder withPosition(Integer position) {
      column.position = position;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      column.auditInfo = auditInfo;
      return this;
    }

    public Column build() {
      column.validate();
      return column;
    }
  }
}
