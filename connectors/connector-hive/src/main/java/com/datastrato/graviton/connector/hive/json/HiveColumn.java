package com.datastrato.graviton.connector.hive.json;

import com.datastrato.graviton.Field;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.catalog.rel.Column;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@EqualsAndHashCode
@ToString
public class HiveColumn implements Column {
  public static final Logger LOG = LoggerFactory.getLogger(HiveColumn.class);
  public static final Field NAME =
          Field.required("name", String.class, "The name of the column");
  public static final Field COMMENT =
          Field.optional("comment", String.class, "The comment of the column");
  public static final Field TYPE =
          Field.required("type", Type.class, "The type of the column");
  public static final Field AUDIT_INFO =
          Field.required("audit_info", AuditInfo.class, "The audit info of the column");

  private String name;

  private String comment;

  private Type dataType;

  private AuditInfo auditInfo;

  // For Jackson Deserialization only.
  public HiveColumn() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, dataType);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @JsonProperty("audit_info")
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type dataType() {
    return dataType;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  public static class Builder {
    private final HiveColumn hiveColumn;

    public Builder() {
      this.hiveColumn = new HiveColumn();
    }

    public HiveColumn.Builder withFieldSchema(org.apache.hadoop.hive.metastore.api.FieldSchema fieldSchema) {
      this.hiveColumn.name = fieldSchema.getName();
      this.hiveColumn.comment = fieldSchema.getComment();

      // todo(xun): add more types convert in the feature
      switch (fieldSchema.getType()) {
        case "string":
          this.hiveColumn.dataType = TypeCreator.NULLABLE.STRING;
        default:
          LOG.error("Unimplemented hive type: " + fieldSchema.getType() + " convert.");
      }

      return this;
    }

    public HiveColumn build() {
      hiveColumn.validate();
      return hiveColumn;
    }
  }
}
