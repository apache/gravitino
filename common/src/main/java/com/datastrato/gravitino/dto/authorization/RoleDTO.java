/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.authorization;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Resource;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.dto.AuditDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Represents a Role Data Transfer Object (DTO). */
public class RoleDTO implements Role {

  @JsonProperty("name")
  private String name;

  @JsonProperty("audit")
  private AuditDTO audit;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("privileges")
  private List<Privilege> privileges;

  @JsonProperty("resource")
  private Resource resource;


  /** Default constructor for Jackson deserialization. */
  protected RoleDTO() {}

  /**
   * Creates a new instance of RoleDTO.
   *
   * @param name The name of the Role DTO.
   * @param properties The properties of the Role DTO.
   * @param audit The audit information of the Role DTO.
   */
  protected RoleDTO(String name, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.audit = audit;
    this.properties = properties;
  }

  /** @return The name of the Group DTO. */
  @Override
  public String name() {
    return name;
  }


  /**
   * The properties of the role. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the role.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * The privileges of the role. All privileges belong to one resource. For example: If the resource
   * is a table, the privileges could be `READ TABLE`, `WRITE TABLE`, etc. If a schema has the
   * privilege of `LOAD TABLE`. It means the role can all tables of the schema.
   *
   * @return The privileges of the role.
   */
  @Override
  public List<Privilege> privileges() {
    return privileges;
  }

  /**
   * The name of the role contains one resource. For example: If the resource is a table, the
   * identifier may be `catalog1.schema1.table1`.
   *
   * @return The resource of the role.
   */
  @Override
  public Resource resource() {
    return resource;
  }

  /** @return The audit information of the Group DTO. */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing a Group DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing a GroupDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the group. */
    protected String name;

    /** The roles of the group. */
    protected List<String> roles = Collections.emptyList();

    /** The audit information of the group. */
    protected AuditDTO audit;

    /**
     * Sets the name of the group.
     *
     * @param name The name of the group.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the roles of the group.
     *
     * @param roles The roles of the group.
     * @return The builder instance.
     */
    public S withRoles(List<String> roles) {
      if (roles != null) {
        this.roles = roles;
      }

      return (S) this;
    }

    /**
     * Sets the audit information of the group.
     *
     * @param audit The audit information of the group.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of GroupDTO using the builder's properties.
     *
     * @return An instance of GroupDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public RoleDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new RoleDTO(name, roles, audit);
    }
  }
}
