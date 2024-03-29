/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.authorization;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.dto.AuditDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Represents a User Data Transfer Object (DTO). */
public class UserDTO implements User {

  @JsonProperty("name")
  private String name;

  @JsonProperty("audit")
  private AuditDTO audit;

  @JsonProperty("roles")
  private List<String> roles;

  /** Default constructor for Jackson deserialization. */
  protected UserDTO() {}

  /**
   * Creates a new instance of UserDTO.
   *
   * @param name The name of the User DTO.
   * @param roles The roles of the User DTO.
   * @param audit The audit information of the User DTO.
   */
  protected UserDTO(String name, List<String> roles, AuditDTO audit) {
    this.name = name;
    this.audit = audit;
    this.roles = roles;
  }

  /** @return The name of the User DTO. */
  @Override
  public String name() {
    return name;
  }

  /**
   * The roles of the user. A user can have multiple roles. Every role binds several privileges.
   *
   * @return The roles of the user.
   */
  @Override
  public List<String> roles() {
    return roles;
  }

  /** @return The audit information of the User DTO. */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing an User DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing a UserDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the user. */
    protected String name;

    /** The roles of the user. */
    protected List<String> roles = Collections.emptyList();

    /** The audit information of the user. */
    protected AuditDTO audit;

    /**
     * Sets the name of the user.
     *
     * @param name The name of the user.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the roles of the user.
     *
     * @param roles The roles of the user.
     * @return The builder instance.
     */
    public S withRoles(List<String> roles) {
      if (roles != null) {
        this.roles = roles;
      }

      return (S) this;
    }

    /**
     * Sets the audit information of the user.
     *
     * @param audit The audit information of the user.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of UserDTO using the builder's properties.
     *
     * @return An instance of UserDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public UserDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new UserDTO(name, roles, audit);
    }
  }
}
