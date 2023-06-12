package com.datastrato.graviton.json;

import com.datastrato.graviton.Field;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.catalog.rel.Column;
import com.datastrato.graviton.meta.catalog.rel.Table;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestTable implements Table {

  public static final Field NAME = Field.required("name", String.class, "The name of the table");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the table");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the table");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the table");

  private String name;

  private String comment;

  private Map<String, String> properties;

  private AuditInfo auditInfo;

  private Column[] columns;

  public TestTable(
      String name,
      String comment,
      Map<String, String> properties,
      AuditInfo auditInfo,
      Column[] columns) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.columns = columns;

    validate();
  }

  // For Jackson Deserialization only.
  public TestTable() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @JsonProperty("audit_info")
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }
}
