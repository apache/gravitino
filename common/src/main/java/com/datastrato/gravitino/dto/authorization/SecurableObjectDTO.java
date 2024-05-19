/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.authorization;

import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Data transfer object representing a securable object. */
public class SecurableObjectDTO implements SecurableObject {

  @JsonProperty("fullName")
  private String fullName;

  @JsonProperty("type")
  private Type type;

  @JsonProperty("privileges")
  private PrivilegeDTO[] privileges;

  private String parent;
  private String name;

  /** Default constructor for Jackson deserialization. */
  protected SecurableObjectDTO() {}

  /**
   * Creates a new instance of SecurableObject DTO.
   *
   * @param privileges The privileges of the ScecurableObject DTO.
   * @param fullName The name of the SecurableObject DTO.
   * @param type The type of the securable object.
   */
  protected SecurableObjectDTO(String fullName, Type type, PrivilegeDTO[] privileges) {
    this.type = type;
    this.fullName = fullName;
    int index = fullName.lastIndexOf(".");

    if (index == -1) {
      this.parent = null;
      this.name = fullName;
    } else {
      this.parent = fullName.substring(0, index);
      this.name = fullName.substring(index + 1);
    }

    this.privileges = privileges;
  }

  @Nullable
  @Override
  public String parentFullName() {
    return parent;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String fullName() {
    return fullName;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public List<Privilege> privileges() {
    if (privileges == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(Arrays.asList(privileges));
  }

  /** @return the builder for creating a new instance of SecurableObjectDTO. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link SecurableObjectDTO}. */
  public static class Builder {
    private String fullName;
    private Type type;
    private PrivilegeDTO[] privileges;

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
     * Sets the privileges of the securable object.
     *
     * @param privileges The privileges of the securable object.
     * @return The builder instance.
     */
    public Builder withPrivileges(PrivilegeDTO[] privileges) {
      this.privileges = privileges;
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

      Preconditions.checkArgument(
          privileges != null && privileges.length != 0, "privileges can't be null or empty");

      SecurableObjectDTO object = new SecurableObjectDTO(fullName, type, privileges);

      return object;
    }
  }
}
