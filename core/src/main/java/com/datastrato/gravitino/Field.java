/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;

/** This class represents a field in the Gravitino framework. */
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
    Preconditions.checkArgument(
        fieldValue != null || optional, "Field " + fieldName + " is required");

    Preconditions.checkArgument(
        fieldValue == null || typeClass.isAssignableFrom(fieldValue.getClass()),
        "Field " + fieldName + " is not of type " + typeClass.getName());
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
      Preconditions.checkArgument(field.fieldName != null, "Field name is required");

      Preconditions.checkArgument(field.typeClass != null, "Field type class is required");

      return field;
    }
  }
}
