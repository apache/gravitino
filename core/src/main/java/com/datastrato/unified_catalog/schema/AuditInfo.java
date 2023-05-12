package com.datastrato.unified_catalog.schema;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public final class AuditInfo implements Entity {
  private static final Field CREATOR = Field.required(
      "creator", String.class, "The name of user who creates the entity");
  private static final Field CREATE_TIME = Field.required(
      "create_time", Instant.class, "The time when the entity is created");
  private static final Field LAST_MODIFIER = Field.optional(
      "last_modifier", String.class, "The name of user who last modifies the entity");
  private static final Field LAST_MODIFIED_TIME = Field.optional(
      "last_modified_time", Instant.class, "The time when the entity is last modified");
  private static final Field LAST_ACCESS_USER = Field.optional(
      "last_access_user", String.class, "The name of user who last accesses the entity");
  private static final Field LAST_ACCESS_TIME = Field.optional(
      "last_access_time", Instant.class, "The time when the entity is last accessed");

  public static final List<Field> SCHEMA = ImmutableList.of(
      CREATOR,
      CREATE_TIME,
      LAST_MODIFIER,
      LAST_MODIFIED_TIME,
      LAST_ACCESS_USER,
      LAST_ACCESS_TIME);

  @JsonProperty("creator")
  private String creator;

  @JsonProperty("create_time")
  private Instant createTime;

  @JsonProperty("last_modifier")
  private String lastModifier;

  @JsonProperty("last_modified_time")
  private Instant lastModifiedTime;

  @JsonProperty("last_access_user")
  private String lastAccessUser;

  @JsonProperty("last_access_time")
  private Instant lastAccessTime;

  private AuditInfo() {}

  @Override
  public void validate() throws IllegalArgumentException {
    CREATOR.validate(creator);
    CREATE_TIME.validate(createTime);

    LAST_MODIFIER.validate(lastModifier);
    LAST_MODIFIED_TIME.validate(lastModifiedTime);
    if (lastModifier != null) {
      Preconditions.checkArgument(lastModifiedTime != null,
          "last_modified_time must be set if last_modifier is set");
    }
    if (lastModifier == null) {
      Preconditions.checkArgument(lastModifiedTime == null,
          "last_modified_time must not be set if last_modifier is not set");
    }

    LAST_ACCESS_USER.validate(lastAccessUser);
    LAST_ACCESS_TIME.validate(lastAccessTime);
    if (lastAccessUser != null) {
      Preconditions.checkArgument(lastAccessTime != null,
          "last_access_time must be set if last_access_user is set");
    }
    if (lastAccessUser == null) {
      Preconditions.checkArgument(lastAccessTime == null,
          "last_access_time must not be set if last_access_user is not set");
    }
  }

  @Override
  public List<Field> schema() {
    return SCHEMA;
  }

  public static class Builder {
    private AuditInfo auditInfo;

    public Builder() {
      this.auditInfo = new AuditInfo();
    }

    public Builder withCreator(String creator) {
      this.auditInfo.creator = creator;
      return this;
    }

    public Builder withCreateTime(Instant createTime) {
      this.auditInfo.createTime = createTime;
      return this;
    }

    public Builder withLastModifier(String lastModifier) {
      this.auditInfo.lastModifier = lastModifier;
      return this;
    }

    public Builder withLastModifiedTime(Instant lastModifiedTime) {
      this.auditInfo.lastModifiedTime = lastModifiedTime;
      return this;
    }

    public Builder withLastAccessUser(String lastAccessUser) {
      this.auditInfo.lastAccessUser = lastAccessUser;
      return this;
    }

    public Builder withLastAccessTime(Instant lastAccessTime) {
      this.auditInfo.lastAccessTime = lastAccessTime;
      return this;
    }

    public AuditInfo build() {
      auditInfo.validate();
      return auditInfo;
    }
  }
}
