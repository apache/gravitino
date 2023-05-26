package com.datastrato.graviton.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
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
public final class Tenant implements Entity, Auditable {

  public static final Field ID =
      Field.required("id", Integer.class, "The unique identifier of the tenant");
  public static final Field NAME = Field.required("name", String.class, "The name of the tenant");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the tenant");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the tenant");
  public static final Field VERSION =
      Field.required("version", SchemaVersion.class, "The version of the tenant");

  @JsonProperty("id")
  private Integer id;

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  @JsonProperty("version")
  private SchemaVersion version;

  private Tenant() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(VERSION, version);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  public static class Builder {
    private final Tenant tenant;

    public Builder() {
      this.tenant = new Tenant();
    }

    public Builder withId(Integer id) {
      this.tenant.id = id;
      return this;
    }

    public Builder withName(String name) {
      this.tenant.name = name;
      return this;
    }

    public Builder withComment(String comment) {
      this.tenant.comment = comment;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      this.tenant.auditInfo = auditInfo;
      return this;
    }

    public Builder withVersion(SchemaVersion version) {
      this.tenant.version = version;
      return this;
    }

    public Tenant build() {
      tenant.validate();
      return tenant;
    }
  }
}
