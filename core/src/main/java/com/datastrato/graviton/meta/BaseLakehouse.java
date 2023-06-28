package com.datastrato.graviton.meta;

import com.datastrato.graviton.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class BaseLakehouse implements Lakehouse, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the lakehouse");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the lakehouse");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the lakehouse");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the lakehouse");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the lakehouse");
  public static final Field SCHEMA_VERSION =
      Field.required("version", SchemaVersion.class, "The schema version of the lakehouse");

  @Getter private Long id;

  private String name;

  @Nullable private String comment;

  @Nullable private Map<String, String> properties;

  private AuditInfo auditInfo;

  @Getter SchemaVersion version;

  private BaseLakehouse() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(SCHEMA_VERSION, version);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  public static class Builder {
    private final BaseLakehouse lakehouse;

    public Builder() {
      lakehouse = new BaseLakehouse();
    }

    public Builder withId(Long id) {
      lakehouse.id = id;
      return this;
    }

    public Builder withName(String name) {
      lakehouse.name = name;
      return this;
    }

    public Builder withComment(String comment) {
      lakehouse.comment = comment;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      lakehouse.properties = properties;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      lakehouse.auditInfo = auditInfo;
      return this;
    }

    public Builder withVersion(SchemaVersion version) {
      lakehouse.version = version;
      return this;
    }

    public BaseLakehouse build() {
      lakehouse.validate();
      return lakehouse;
    }
  }
}
