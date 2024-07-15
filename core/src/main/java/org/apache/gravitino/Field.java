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
package org.apache.gravitino;

import lombok.EqualsAndHashCode;

/** This class represents a field in the Apache Gravitino framework. */
@EqualsAndHashCode
public class Field {

  private String fieldName;

  private Class<?> typeClass;

  private String description;

  private boolean optional;

  private Field() {}

  /**
   * Creates a required field instance.
   *
   * @param fieldName The name of the field.
   * @param typeClass The type class of the field.
   * @param description The description of the field.
   * @return A required Field instance.
   */
  public static Field required(String fieldName, Class<?> typeClass, String description) {
    return new Builder(false)
        .withName(fieldName)
        .withTypeClass(typeClass)
        .withDescription(description)
        .build();
  }

  /**
   * Creates an optional field instance.
   *
   * @param fieldName The name of the field.
   * @param typeClass The type class of the field.
   * @param description The description of the field.
   * @return An optional Field instance.
   */
  public static Field optional(String fieldName, Class<?> typeClass, String description) {
    return new Builder(true)
        .withName(fieldName)
        .withTypeClass(typeClass)
        .withDescription(description)
        .build();
  }

  /**
   * Creates a required field instance.
   *
   * @param fieldName The name of the field.
   * @param typeClass The type class of the field.
   * @return A required Field instance.
   */
  public static Field required(String fieldName, Class<?> typeClass) {
    return new Builder(false).withName(fieldName).withTypeClass(typeClass).build();
  }

  /**
   * Creates an optional field instance.
   *
   * @param fieldName The name of the field.
   * @param typeClass The type class of the field.
   * @return An optional Field instance.
   */
  public static Field optional(String fieldName, Class<?> typeClass) {
    return new Builder(true).withName(fieldName).withTypeClass(typeClass).build();
  }

  /**
   * Validates a field value according to the field's requirements.
   *
   * @param fieldValue The value to be validated.
   * @param <T> The type of the field value.
   * @throws IllegalArgumentException If the field value is invalid.
   */
  public <T> void validate(T fieldValue) {
    if (fieldValue == null && !optional) {
      throw new IllegalArgumentException("Field " + fieldName + " is required");
    }

    if (fieldValue != null && !typeClass.isAssignableFrom(fieldValue.getClass())) {
      throw new IllegalArgumentException(
          "Field " + fieldName + " is not of type " + typeClass.getName());
    }
  }

  /** Builder class for creating Field instances. */
  public static class Builder {
    private final Field field;

    /**
     * Constructs a Field Builder with the specified optionality.
     *
     * @param isOptional Set to true for an optional field, false for a required field.
     */
    public Builder(boolean isOptional) {
      field = new Field();
      field.optional = isOptional;
    }

    /**
     * Sets the name of the field.
     *
     * @param name The name of the field.
     * @return The Builder instance.
     */
    public Builder withName(String name) {
      field.fieldName = name;
      return this;
    }

    /**
     * Sets the type class of the field.
     *
     * @param typeClass The type class of the field.
     * @return The Builder instance.
     */
    public Builder withTypeClass(Class<?> typeClass) {
      field.typeClass = typeClass;
      return this;
    }

    /**
     * Sets the description of the field.
     *
     * @param description The description of the field.
     * @return The Builder instance.
     */
    public Builder withDescription(String description) {
      field.description = description;
      return this;
    }

    /**
     * Builds and returns the configured Field instance.
     *
     * @return The created Field instance.
     * @throws IllegalArgumentException If the field attributes are not properly set.
     */
    public Field build() {
      if (field.fieldName == null) {
        throw new IllegalArgumentException("Field name is required");
      }

      if (field.typeClass == null) {
        throw new IllegalArgumentException("Field type class is required");
      }

      return field;
    }
  }
}
