package com.datastrato.unified_catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.*;
import javax.annotation.Nullable;
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

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the table");
  public static final Field ZONE_ID =
      Field.required(
          "zone_id", Long.class, "The unique identifier of the zone which this table belongs to");
  public static final Field NAME = Field.required("name", String.class, "The name of the table");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the table");
  public static final Field TABLE_TYPE =
      Field.required("type", TableType.class, "The type of the table");
  public static final Field SNAPSHOT_ID =
      Field.required("snapshot_id", Long.class, "The snapshot id of the table");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the table");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the table");
  public static final Field EXTRA_INFO =
      Field.required("extra_info", hasExtraInfo.ExtraInfo.class, "The extra info of the table");

  @JsonProperty("id")
  private Long id;

  @JsonProperty("zone_id")
  private Long zoneId;

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("type")
  private TableType type;

  @JsonProperty("snapshot_id")
  private Long snapshotId;

  @Nullable
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
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(ZONE_ID, zoneId);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TABLE_TYPE, type);
    fields.put(SNAPSHOT_ID, snapshotId);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(EXTRA_INFO, extraInfo);

    return Collections.unmodifiableMap(fields);
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

    public Builder withId(Long id) {
      table.id = id;
      return this;
    }

    public Builder withZoneId(Long zoneId) {
      table.zoneId = zoneId;
      return this;
    }

    public Builder withName(String name) {
      table.name = name;
      return this;
    }

    public Builder withComment(String comment) {
      table.comment = comment;
      return this;
    }

    public Builder withType(TableType type) {
      table.type = type;
      return this;
    }

    public Builder withSnapshotId(Long snapshotId) {
      table.snapshotId = snapshotId;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      table.properties = properties;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      table.auditInfo = auditInfo;
      return this;
    }

    public Builder withExtraInfo(hasExtraInfo.ExtraInfo extraInfo) {
      table.extraInfo = extraInfo;
      return this;
    }

    public Builder withColumns(List<Column> columns) {
      table.columns = columns;
      return this;
    }

    public Table build() {
      table.validate();
      return table;
    }
  }
}
