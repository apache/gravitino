/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.meta;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;

/** Audit information associated with an entity. */
@EqualsAndHashCode
@ToString
public final class AuditInfo implements Audit, Entity {

  public static final Field CREATOR =
      Field.optional("creator", String.class, "The name of the user who created the entity");
  public static final Field CREATE_TIME =
      Field.optional("create_time", Instant.class, "The time when the entity was created");
  public static final Field LAST_MODIFIER =
      Field.optional(
          "last_modifier", String.class, "The name of the user who last modified the entity");
  public static final Field LAST_MODIFIED_TIME =
      Field.optional(
          "last_modified_time", Instant.class, "The time when the entity was last modified");

  public static final AuditInfo EMPTY = AuditInfo.builder().build();

  public static Builder builder() {
    return new Builder();
  }

  @Nullable private String creator;

  @Nullable private Instant createTime;

  @Nullable private String lastModifier;

  @Nullable private Instant lastModifiedTime;

  private AuditInfo() {}

  /**
   * Validates the audit information.
   *
   * @throws IllegalArgumentException If the validation fails.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    CREATOR.validate(creator);
    CREATE_TIME.validate(createTime);

    LAST_MODIFIER.validate(lastModifier);
    LAST_MODIFIED_TIME.validate(lastModifiedTime);
  }

  /**
   * Retrieves a map of fields.
   *
   * @return An unmodifiable map containing the entity's fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(CREATOR, creator);
    fields.put(CREATE_TIME, createTime);
    fields.put(LAST_MODIFIER, lastModifier);
    fields.put(LAST_MODIFIED_TIME, lastModifiedTime);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Retrieves the creator's name.
   *
   * @return The name of the creator.
   */
  @Override
  public String creator() {
    return creator;
  }

  /**
   * Retrieves the creation time.
   *
   * @return the creation time as an {@link Instant}.
   */
  @Override
  public Instant createTime() {
    return createTime;
  }

  /**
   * Retrieves the last modifier's name.
   *
   * @return the name of the last modifier, or null if not set.
   */
  @Override
  public String lastModifier() {
    return lastModifier;
  }

  /**
   * Retrieves the last modified time.
   *
   * @return the last modified time as an {@link Instant}, or null if not set.
   */
  @Override
  public Instant lastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * Retrieves the type of the entity.
   *
   * @return the {@link EntityType#AUDIT} value.
   */
  @Override
  public EntityType type() {
    return EntityType.AUDIT;
  }

  /**
   * Merges the audit information with another audit information. If the {@code overwrite} flag is
   * set to {@code true} or the field is null, the values from the other audit information will
   * overwrite the values of this audit information, otherwise the values of this audit information
   * will be preserved.
   *
   * @param other the other audit information.
   * @param overwrite the overwrite flag.
   * @return the merged audit information.
   */
  public AuditInfo merge(AuditInfo other, boolean overwrite) {
    if (other == null) {
      return this;
    }

    this.creator = overwrite || this.creator == null ? other.creator : creator;
    this.createTime = overwrite || this.createTime == null ? other.createTime : createTime;
    this.lastModifier = overwrite || this.lastModifier == null ? other.lastModifier : lastModifier;
    this.lastModifiedTime =
        overwrite || this.lastModifiedTime == null ? other.lastModifiedTime : lastModifiedTime;

    return this;
  }

  /** Builder class for creating instances of {@link AuditInfo}. */
  public static class Builder {
    @Nullable private String creator;
    @Nullable private Instant createTime;
    @Nullable private String lastModifier;
    @Nullable private Instant lastModifiedTime;

    /** Constructs a new {@link Builder}. */
    private Builder() {}

    /**
     * Sets the creator's name.
     *
     * @param creator the name of the creator.
     * @return the builder instance.
     */
    public Builder withCreator(String creator) {
      this.creator = creator;
      return this;
    }

    /**
     * Sets the creation time.
     *
     * @param createTime the creation time as an {@link Instant}.
     * @return the builder instance.
     */
    public Builder withCreateTime(Instant createTime) {
      this.createTime = createTime;
      return this;
    }

    /**
     * Sets the modifier's name.
     *
     * @param lastModifier the name of the modifier.
     * @return the builder instance.
     */
    public Builder withLastModifier(String lastModifier) {
      this.lastModifier = lastModifier;
      return this;
    }

    /**
     * Sets the last modified time.
     *
     * @param lastModifiedTime the last modified time as an {@link Instant}.
     * @return the builder instance.
     */
    public Builder withLastModifiedTime(Instant lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return this;
    }

    /**
     * Builds the {@link AuditInfo} instance after validation.
     *
     * @return the constructed and validated {@link AuditInfo} instance.
     */
    public AuditInfo build() {
      AuditInfo auditInfo = new AuditInfo();
      auditInfo.creator = this.creator;
      auditInfo.createTime = this.createTime;
      auditInfo.lastModifier = this.lastModifier;
      auditInfo.lastModifiedTime = this.lastModifiedTime;

      auditInfo.validate();
      return auditInfo;
    }
  }
}
