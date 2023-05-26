package com.datastrato.graviton.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.time.Instant;
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
public final class AuditInfo implements Entity {
  public static final Field CREATOR =
      Field.required("creator", String.class, "The name of user who creates the entity");
  public static final Field CREATE_TIME =
      Field.required("create_time", Instant.class, "The time when the entity is created");
  public static final Field LAST_MODIFIER =
      Field.optional(
          "last_modifier", String.class, "The name of user who last modifies the entity");
  public static final Field LAST_MODIFIED_TIME =
      Field.optional(
          "last_modified_time", Instant.class, "The time when the entity is last modified");
  public static final Field LAST_ACCESS_USER =
      Field.optional(
          "last_access_user", String.class, "The name of user who last accesses the entity");
  public static final Field LAST_ACCESS_TIME =
      Field.optional(
          "last_access_time", Instant.class, "The time when the entity is last accessed");

  @JsonProperty("creator")
  private String creator;

  @JsonProperty("create_time")
  private Instant createTime;

  @Nullable
  @JsonProperty("last_modifier")
  private String lastModifier;

  @Nullable
  @JsonProperty("last_modified_time")
  private Instant lastModifiedTime;

  @Nullable
  @JsonProperty("last_access_user")
  private String lastAccessUser;

  @Nullable
  @JsonProperty("last_access_time")
  private Instant lastAccessTime;

  private AuditInfo() {}

  @Override
  public void validate() throws IllegalArgumentException {
    CREATOR.validate(creator);
    CREATE_TIME.validate(createTime);

    Preconditions.checkArgument(
        lastModifier == null && lastModifiedTime == null
            || lastModifier != null && lastModifiedTime != null,
        "last_modifier and last_modified_time must be both set or both not set");

    Preconditions.checkArgument(
        lastAccessUser == null && lastAccessTime == null
            || lastAccessUser != null && lastAccessTime != null,
        "last_access_user and last_access_time must be both set or both not set");
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(CREATOR, creator);
    fields.put(CREATE_TIME, createTime);
    fields.put(LAST_MODIFIER, lastModifier);
    fields.put(LAST_MODIFIED_TIME, lastModifiedTime);
    fields.put(LAST_ACCESS_USER, lastAccessUser);
    fields.put(LAST_ACCESS_TIME, lastAccessTime);

    return Collections.unmodifiableMap(fields);
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
