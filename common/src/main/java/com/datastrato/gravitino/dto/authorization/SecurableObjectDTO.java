/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.authorization;

import com.datastrato.gravitino.authorization.SecurableObject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Data transfer object representing a securable object. */
public class SecurableObjectDTO implements SecurableObject {

  @JsonProperty("fullName")
  private String fullName;

  @JsonProperty("type")
  private Type type;

  /** Default constructor for Jackson deserialization. */
  protected SecurableObjectDTO() {}

  /**
   * Creates a new instance of RoleDTO.
   *
   * @param fullName The name of the Role DTO.
   * @param type The type of the securable object.
   */
  protected SecurableObjectDTO(String fullName, Type type) {
    this.type = type;
    this.fullName = fullName;
  }

  @Nullable
  @Override
  public SecurableObject parent() {
    throw new UnsupportedOperationException(
        "SecurableObjectDTO doesn't support the method `parent`");
  }

  @Override
  public String name() {
    throw new UnsupportedOperationException("SecurableObjectDTO doesn't support the method `name`");
  }

  @Override
  public String fullName() {
    return fullName;
  }

  @Override
  public Type type() {
    return type;
  }

  /** @return the builder for creating a new instance of SecurableObjectDTO. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link SecurableObjectDTO}. */
  public static class Builder {
    private String fullName;
    private Type type;

    /**
     * Sets the full name of the securable object.
     *
     * @param fullName The full name of the securable object.
     * @return The builder instance.
     */
    public Builder withFullName(String fullName) {
      this.fullName = fullName;
      return this;
    }

    /**
     * Sets the type of the securable object.
     *
     * @param type The type of the securable object.
     * @return The builder instance.
     */
    public Builder withType(Type type) {
      this.type = type;
      return this;
    }

    /**
     * Builds an instance of SecurableObjectDTO using the builder's properties.
     *
     * @return An instance of SecurableObjectDTO.
     * @throws IllegalArgumentException If the full name or type are not set.
     */
    public SecurableObjectDTO build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(fullName), "full name cannot be null or empty");

      Preconditions.checkArgument(type != null, "type cannot be null");

      return new SecurableObjectDTO(fullName, type);
    }
  }
}
