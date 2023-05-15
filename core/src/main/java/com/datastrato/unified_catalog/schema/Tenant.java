package com.datastrato.unified_catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public final class Tenant implements Entity, Auditable {

  private static final Field ID =
      Field.required("id", Integer.class, "The unique identifier of the tenant");
  private static final Field NAME = Field.required("name", String.class, "The name of the tenant");
  private static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the tenant");
  private static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the tenant");
  private static final Field VERSION =
      Field.required("version", SchemaVersion.class, "The version of the tenant");

  @JsonProperty("id")
  private Integer id;

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  @JsonProperty("version")
  private SchemaVersion version;

  private Tenant() {}

  @Override
  public Map<Field, Object> fields() {
    return new ImmutableMap.Builder<Field, Object>()
        .put(ID, id)
        .put(NAME, name)
        .put(COMMENT, comment)
        .put(AUDIT_INFO, auditInfo)
        .put(VERSION, version)
        .build();
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

    public Builder id(Integer id) {
      this.tenant.id = id;
      return this;
    }

    public Builder name(String name) {
      this.tenant.name = name;
      return this;
    }

    public Builder comment(String comment) {
      this.tenant.comment = comment;
      return this;
    }

    public Builder auditInfo(AuditInfo auditInfo) {
      this.tenant.auditInfo = auditInfo;
      return this;
    }

    public Builder version(SchemaVersion version) {
      this.tenant.version = version;
      return this;
    }

    public Tenant build() {
      tenant.validate();
      return tenant;
    }
  }
}
