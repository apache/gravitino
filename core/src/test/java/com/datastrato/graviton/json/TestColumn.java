package com.datastrato.graviton.json;

import com.datastrato.graviton.Field;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.catalog.rel.Column;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class TestColumn implements Column {

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

  public TestColumn(String name, String comment, Type dataType, AuditInfo auditInfo) {
    this.name = name;
    this.comment = comment;
    this.dataType = dataType;
    this.auditInfo = auditInfo;

    validate();
  }

  // For Jackson Deserialization only.
  public TestColumn() {}

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
}
