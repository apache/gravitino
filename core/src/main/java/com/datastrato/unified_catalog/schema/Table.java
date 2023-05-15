package com.datastrato.unified_catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class Table implements Entity, Auditable, hasExtraInfo {
  enum TableType {
    VIRTUAL("VIRTUAL"),
    VIEW("VIEW"),
    EXTERNAL("EXTERNAL"),
    MANAGED("MANAGED");

    private final String typeStr;

    TableType(String typeStr) {
      this.typeStr = typeStr;
    }

    public String getTypeStr() {
      return typeStr;
    }
  }

  private static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the table");
  private static final Field ZONE_ID =
      Field.required(
          "zone_id", Long.class, "The unique identifier of the zone which this table belongs to");
  private static final Field NAME = Field.required("name", String.class, "The name of the table");
  private static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the table");
  private static final Field TABLE_TYPE =
      Field.required("type", TableType.class, "The type of the table");
  private static final Field SNAPSHOT_ID =
      Field.required("snapshot_id", Long.class, "The snapshot id of the table");
  private static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the table");
  private static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the table");
  private static final Field EXTRA_INFO =
      Field.optional("extra_info", hasExtraInfo.ExtraInfo.class, "The extra info of the table");

  @JsonProperty("id")
  private Long id;

  @JsonProperty("zone_id")
  private Long zoneId;

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("type")
  private TableType type;

  @JsonProperty("snapshot_id")
  private Long snapshotId;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  @JsonProperty("extra_info")
  private hasExtraInfo.ExtraInfo extraInfo;

  @JsonProperty("columns")
  private List<Column> columns;

  private Table() {}

  @Override
  public Map<Field, Object> fields() {
    return new ImmutableMap.Builder<Field, Object>()
        .put(ID, id)
        .put(ZONE_ID, zoneId)
        .put(NAME, name)
        .put(COMMENT, comment)
        .put(TABLE_TYPE, type)
        .put(SNAPSHOT_ID, snapshotId)
        .put(PROPERTIES, properties)
        .put(AUDIT_INFO, auditInfo)
        .put(EXTRA_INFO, extraInfo)
        .build();
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public hasExtraInfo.ExtraInfo extraInfo() {
    return extraInfo;
  }

  public static class Builder {
    private final Table table;

    public Builder() {
      table = new Table();
    }

    public Builder id(Long id) {
      table.id = id;
      return this;
    }

    public Builder zoneId(Long zoneId) {
      table.zoneId = zoneId;
      return this;
    }

    public Builder name(String name) {
      table.name = name;
      return this;
    }

    public Builder comment(String comment) {
      table.comment = comment;
      return this;
    }

    public Builder type(TableType type) {
      table.type = type;
      return this;
    }

    public Builder snapshotId(Long snapshotId) {
      table.snapshotId = snapshotId;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      table.properties = properties;
      return this;
    }

    public Builder auditInfo(AuditInfo auditInfo) {
      table.auditInfo = auditInfo;
      return this;
    }

    public Builder extraInfo(hasExtraInfo.ExtraInfo extraInfo) {
      table.extraInfo = extraInfo;
      return this;
    }

    public Builder columns(List<Column> columns) {
      table.columns = columns;
      return this;
    }

    public Table build() {
      table.validate();
      return table;
    }
  }
}
