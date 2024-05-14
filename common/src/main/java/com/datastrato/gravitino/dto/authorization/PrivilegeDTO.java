/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.authorization;

import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Privileges;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/** Data transfer object representing a privilege. */
public class PrivilegeDTO implements Privilege {

  @JsonProperty("name")
  private Name name;

  @JsonProperty("effect")
  private Effect effect;

  /** Default constructor for Jackson deserialization. */
  protected PrivilegeDTO() {}

  /**
   * Creates a new instance of PrivilegeDTO.
   *
   * @param name The name of the Privilege DTO.
   * @param effect The effect of the Privilege DTO.
   */
  protected PrivilegeDTO(Name name, Effect effect) {
    this.name = name;
    this.effect = effect;
  }

  @Override
  public Name name() {
    return name;
  }

  @Override
  public String simpleString() {
    if (Effect.ALLOW.equals(effect)) {
      return Privileges.allowPrivilegeFromName(name).simpleString();
    } else {
      return Privileges.denyPrivilegeFromName(name).simpleString();
    }
  }

  @Override
  public Effect effect() {
    return effect;
  }

  /** @return the builder for creating a new instance of PrivilegeDTO. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link PrivilegeDTO}. */
  public static class Builder {

    private Name name;
    private Effect effect;

    /**
     * Sets the name of the privilege.
     *
     * @param name The name of the privilege.
     * @return The builder instance.
     */
    public Builder withName(Name name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the effect of the privilege.
     *
     * @param effect The effect of the privilege.
     * @return The builder instance.
     */
    public Builder withEffect(Effect effect) {
      this.effect = effect;
      return this;
    }

    /**
     * Builds an instance of PrivilegeDTO using the builder's properties.
     *
     * @return An instance of PrivilegeDTO.
     * @throws IllegalArgumentException If the name or effect are not set.
     */
    public PrivilegeDTO build() {
      Preconditions.checkArgument(name != null, "name cannot be null");
      Preconditions.checkArgument(effect != null, "effect cannot be null");
      return new PrivilegeDTO(name, effect);
    }
  }
}
