/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public final class AuditInfo implements Audit, Entity {
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

  private String creator;

  private Instant createTime;

  @Nullable private String lastModifier;

  @Nullable private Instant lastModifiedTime;

  private AuditInfo() {}

  @Override
  public void validate() throws IllegalArgumentException {
    CREATOR.validate(creator);
    CREATE_TIME.validate(createTime);

    Preconditions.checkArgument(
        lastModifier == null && lastModifiedTime == null
            || lastModifier != null && lastModifiedTime != null,
        "last_modifier and last_modified_time must be both set or both not set");
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(CREATOR, creator);
    fields.put(CREATE_TIME, createTime);
    fields.put(LAST_MODIFIER, lastModifier);
    fields.put(LAST_MODIFIED_TIME, lastModifiedTime);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String creator() {
    return creator;
  }

  @Override
  public Instant createTime() {
    return createTime;
  }

  @Override
  public String lastModifier() {
    return lastModifier;
  }

  @Override
  public Instant lastModifiedTime() {
    return lastModifiedTime;
  }

  @Override
  public EntityType type() {
    return EntityType.AUDIT;
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

    public AuditInfo build() {
      auditInfo.validate();
      return auditInfo;
    }
  }
}
