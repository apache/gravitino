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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.policy.PolicyManager;

/**
 * A generic entity that mainly used for temporary transactions or internal operations.
 *
 * <p>For example, it can be used to represent different types of entities for {@link
 * PolicyManager#listMetadataObjectsForPolicy(String, String)} intermediate result then can be
 * converted to metadata objects.
 */
@EqualsAndHashCode
@ToString
public class GenericEntity implements Entity, HasIdentifier {

  public static final Field ID = Field.required("id", Long.class, "The unique identifier");
  public static final Field ENTITY_TYPE =
      Field.required("entityType", EntityType.class, "The entity's type");

  private Long id;
  private EntityType entityType;
  private String name;

  private GenericEntity() {}

  /**
   * A map of fields and their corresponding values.
   *
   * @return An unmodifiable map containing the entity's fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(ENTITY_TYPE, type());

    return Collections.unmodifiableMap(fields);
  }

  /**
   * The name of the entity. May be null if not set.
   *
   * @return The name of the entity.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The unique id of the entity.
   *
   * @return The unique id of the entity.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Retrieves the type of the entity.
   *
   * @return the type of the entity.
   */
  @Override
  public EntityType type() {
    return entityType;
  }

  /** Builder class for creating instances of {@link GenericEntity}. */
  public static class Builder {
    private final GenericEntity entity;

    /** Constructs a new {@link Builder}. */
    private Builder() {
      entity = new GenericEntity();
    }

    /**
     * Sets the unique identifier of the entity.
     *
     * @param id the unique identifier of the entity.
     * @return the builder instance.
     */
    public Builder withId(Long id) {
      entity.id = id;
      return this;
    }

    /**
     * Sets the name of the entity.
     *
     * @param name the name of the entity.
     * @return the builder instance.
     */
    public Builder withName(String name) {
      entity.name = name;
      return this;
    }

    /**
     * Sets the type of the entity.
     *
     * @param type the type of the entity.
     * @return the builder instance.
     */
    public Builder withEntityType(EntityType type) {
      entity.entityType = type;
      return this;
    }

    /**
     * Builds the {@link GenericEntity} instance after validation.
     *
     * @return the constructed and validated {@link GenericEntity} instance.
     */
    public GenericEntity build() {
      GenericEntity genericEntity = new GenericEntity();
      genericEntity.id = entity.id;
      genericEntity.name = entity.name;
      genericEntity.entityType = entity.entityType;
      genericEntity.validate();
      return genericEntity;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
