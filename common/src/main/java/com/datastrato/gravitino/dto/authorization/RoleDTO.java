/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.authorization;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.dto.AuditDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Represents a Role Data Transfer Object (DTO). */
public class RoleDTO implements Role {

  @JsonProperty("name")
  private String name;

  @JsonProperty("audit")
  private AuditDTO audit;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("securableObjects")
  private SecurableObjectDTO[] securableObjects;

  /** Default constructor for Jackson deserialization. */
  protected RoleDTO() {}

  /**
   * Creates a new instance of RoleDTO.
   *
   * @param name The name of the Role DTO.
   * @param properties The properties of the Role DTO.
   * @param securableObjects The securable objects of the Role DTO.
   * @param audit The audit information of the Role DTO.
   */
  protected RoleDTO(
      String name,
      Map<String, String> properties,
      SecurableObjectDTO[] securableObjects,
      AuditDTO audit) {
    this.name = name;
    this.audit = audit;
    this.properties = properties;
    this.securableObjects = securableObjects;
  }

  /** @return The name of the Role DTO. */
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
   * The resource represents a special kind of entity with a unique identifier. All resources are
   * organized by tree structure. For example: If the resource is a table, the identifier may be
   * `catalog1.schema1.table1`.
   *
   * @return The securable object of the role.
   */
  @Override
  public List<SecurableObject> securableObjects() {
    return Arrays.asList(securableObjects);
  }

  /** @return The audit information of the Role DTO. */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing a Role DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing a RoleDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the role. */
    protected String name;

    /** The audit information of the role. */
    protected AuditDTO audit;

    /** The properties of the role. */
    protected Map<String, String> properties;

    /** The securable objects of the role. */
    protected SecurableObjectDTO[] securableObjects;

    /**
     * Sets the name of the role.
     *
     * @param name The name of the role.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the properties of the role.
     *
     * @param properties The properties of the role.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      if (properties != null) {
        this.properties = properties;
      }

      return (S) this;
    }

    /**
     * Sets the securable objects of the role.
     *
     * @param securableObjects The securableObjects of the role.
     * @return The builder instance.
     */
    public S withSecurableObjects(SecurableObjectDTO[] securableObjects) {
      this.securableObjects = securableObjects;
      return (S) this;
    }

    /**
     * Sets the audit information of the role.
     *
     * @param audit The audit information of the role.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Builds an instance of RoleDTO using the builder's properties.
     *
     * @return An instance of RoleDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public RoleDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      Preconditions.checkArgument(
          securableObjects != null && securableObjects.length != 0,
          "securable objects can't null or empty");

      return new RoleDTO(name, properties, securableObjects, audit);
    }
  }
}
