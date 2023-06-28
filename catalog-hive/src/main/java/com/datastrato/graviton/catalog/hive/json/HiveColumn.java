package com.datastrato.graviton.catalog.hive.json;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EqualsAndHashCode
@ToString
public class HiveColumn implements Column, Entity, HasIdentifier {
  public static final Logger LOG = LoggerFactory.getLogger(HiveColumn.class);
  public static final Field NAME = Field.required("name", String.class, "The name of the column");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the column");
  public static final Field TYPE = Field.required("type", Type.class, "The type of the column");
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

    public HiveColumn.Builder withFieldSchema(
        org.apache.hadoop.hive.metastore.api.FieldSchema fieldSchema) {
      this.hiveColumn.name = fieldSchema.getName();
      this.hiveColumn.comment = fieldSchema.getComment();

      // todo(xun): add more types convert in the feature
      if (fieldSchema.getType().equals("string")) {
        this.hiveColumn.dataType = TypeCreator.NULLABLE.STRING;
      }
      LOG.error("Unimplemented hive type: " + fieldSchema.getType() + " convert.");

      return this;
    }

    public HiveColumn build() {
      hiveColumn.validate();
      return hiveColumn;
    }
  }
}
